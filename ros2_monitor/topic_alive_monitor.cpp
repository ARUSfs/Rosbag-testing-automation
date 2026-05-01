/**
 * topic_alive_monitor.cpp
 *
 * Nodo rclcpp que monitoriza la liveness de un topic ROS2.
 * Se comunica con el wrapper Python exclusivamente por stdout (JSON lines).
 *
 * Uso:
 *   ros2 run ros2_monitor topic_alive_monitor <topic> <timeout_s> [disc_timeout_s]
 *
 * Protocolo de salida (una línea JSON por evento):
 *   {"event":"subscribed",    "topic":"/foo", "elapsed":0.12, "type":"std_msgs/msg/String"}
 *   {"event":"first_message", "topic":"/foo", "elapsed":0.45}
 *   {"event":"silence",       "topic":"/foo", "elapsed":6.20}
 *   {"event":"recovered",     "topic":"/foo", "elapsed":8.10}
 *   {"event":"not_found",     "topic":"/foo", "elapsed":5.00}
 *   {"event":"error",         "topic":"/foo", "elapsed":0.10, "message":"..."}
 *   {"event":"shutdown",      "topic":"/foo", "elapsed":12.3}
 */

#include <atomic>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>

#include "rclcpp/rclcpp.hpp"

using namespace std::chrono_literals;

// ─── Shutdown flag global ────────────────────────────────────────────────────

static std::atomic<bool> g_shutdown{false};

static void on_signal(int) { g_shutdown = true; }

// ─── Helpers JSON ────────────────────────────────────────────────────────────

static std::string json_event(
    const std::string & event,
    const std::string & topic,
    double              elapsed,
    const std::string & extra_key = "",
    const std::string & extra_val = "")
{
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(2);
    ss << "{\"event\":\""   << event   << "\""
       << ",\"topic\":\""   << topic   << "\""
       << ",\"elapsed\":"   << elapsed;
    if (!extra_key.empty())
        ss << ",\"" << extra_key << "\":\"" << extra_val << "\"";
    ss << "}";
    return ss.str();
}

// ─── Nodo monitor ────────────────────────────────────────────────────────────

class TopicAliveMonitor : public rclcpp::Node
{
public:
    TopicAliveMonitor(
        const std::string & topic,
        double              timeout_s,
        double              discovery_timeout_s)
    : Node("topic_alive_monitor_" + sanitize(topic)),
      topic_(topic),
      timeout_s_(timeout_s),
      discovery_timeout_s_(discovery_timeout_s)
    {
        start_time_ = now();

        // Discovery: sondeo cada 500 ms hasta encontrar el topic
        discovery_timer_ = create_wall_timer(
            500ms, std::bind(&TopicAliveMonitor::on_discovery_tick, this));
    }

private:
    // ── Estado ───────────────────────────────────────────────────────────────

    const std::string                        topic_;
    const double                             timeout_s_;
    const double                             discovery_timeout_s_;
    rclcpp::Time                             start_time_;

    // Suscripción genérica (cualquier tipo de mensaje)
    std::shared_ptr<rclcpp::GenericSubscription> subscription_;
    rclcpp::TimerBase::SharedPtr             discovery_timer_;
    rclcpp::TimerBase::SharedPtr             liveness_timer_;

    // Accedidos desde el hilo del executor y el callback de msg → mutex
    std::mutex      mtx_;
    rclcpp::Time    last_msg_time_;
    bool            received_first_{false};
    bool            subscribed_{false};
    bool            in_grace_period_{false};
    rclcpp::Time    grace_end_;
    bool            failure_active_{false}; // evita emitir silence repetido

    // ── Utilidades ───────────────────────────────────────────────────────────

    static std::string sanitize(const std::string & s)
    {
        std::string out;
        out.reserve(s.size());
        for (char c : s)
            out += (c == '/' || c == '-' || c == ' ') ? '_' : c;
        // Quitar guiones bajos iniciales que generaría un topic tipo "/foo"
        size_t start = out.find_first_not_of('_');
        return (start == std::string::npos) ? "node" : out.substr(start);
    }

    double elapsed() const
    {
        return (now() - start_time_).seconds();
    }

    // emit() es la única función que escribe a stdout.
    // Flush inmediato para que el wrapper Python reciba línea a línea.
    void emit(const std::string & line)
    {
        std::cout << line << '\n';
        std::cout.flush();
    }

    // ── Discovery ────────────────────────────────────────────────────────────

    void on_discovery_tick()
    {
        if (subscribed_) return;

        const double t = elapsed();

        if (t > discovery_timeout_s_) {
            emit(json_event("not_found", topic_, t));
            discovery_timer_->cancel();
            g_shutdown = true;
            return;
        }

        // Buscar el topic en la lista de topics activos
        const auto topics_map = get_topic_names_and_types();
        const auto it = topics_map.find(topic_);
        if (it == topics_map.end() || it->second.empty()) {
            return; // Todavía no está disponible, volvemos a intentarlo
        }

        const std::string & type_str = it->second.front();

        try {
            // create_generic_subscription no necesita saber el tipo en tiempo
            // de compilación: acepta cualquier SerializedMessage.
            subscription_ = create_generic_subscription(
                topic_,
                type_str,
                rclcpp::QoS(10),
                [this](std::shared_ptr<rclcpp::SerializedMessage> /*msg*/) {
                    on_message_received();
                });

            subscribed_     = true;
            in_grace_period_ = true;

            {
                std::lock_guard<std::mutex> lk(mtx_);
                // Periodo de gracia = timeout_s_ desde la suscripción
                grace_end_ = now() + rclcpp::Duration::from_seconds(timeout_s_);
            }

            discovery_timer_->cancel();

            // Timer de liveness: se dispara cada timeout/2 segundos
            const auto interval_ns = static_cast<int64_t>(
                (timeout_s_ / 2.0) * 1e9);
            liveness_timer_ = create_wall_timer(
                std::chrono::nanoseconds(interval_ns),
                std::bind(&TopicAliveMonitor::on_liveness_tick, this));

            emit(json_event("subscribed", topic_, t, "type", type_str));
        }
        catch (const std::exception & e) {
            emit(json_event("error", topic_, t, "message", e.what()));
            discovery_timer_->cancel();
            g_shutdown = true;
        }
    }

    // ── Callback de mensaje ───────────────────────────────────────────────────

    void on_message_received()
    {
        std::lock_guard<std::mutex> lk(mtx_);
        last_msg_time_ = now();

        if (!received_first_) {
            received_first_ = true;
            // emit desde callback ROS: seguro porque cout es thread-safe en
            // escrituras atómicas de línea (y usamos flush explícito)
            emit(json_event("first_message", topic_, elapsed()));
        }
    }

    // ── Check de liveness ────────────────────────────────────────────────────

    void on_liveness_tick()
    {
        std::lock_guard<std::mutex> lk(mtx_);

        // Todavía en periodo de gracia: no evaluar
        if (in_grace_period_) {
            if (now() < grace_end_) return;
            in_grace_period_ = false;
        }

        // Sin mensajes aún (raro si el topic existe, pero defensivo)
        if (!received_first_) return;

        const double since_last = (now() - last_msg_time_).seconds();

        if (since_last > timeout_s_) {
            if (!failure_active_) {
                emit(json_event("silence", topic_, since_last));
                failure_active_ = true;
            }
            // Si failure_active_ ya era true, no se emite de nuevo
            // (el wrapper Python ya registró el fallo)
        } else {
            if (failure_active_) {
                // El topic se ha recuperado
                emit(json_event("recovered", topic_, elapsed()));
                failure_active_ = false;
            }
        }
    }
};

// ─── main ────────────────────────────────────────────────────────────────────

int main(int argc, char ** argv)
{
    // argv[1] = topic, argv[2] = timeout_s, argv[3] = discovery_timeout_s (opt.)
    if (argc < 3) {
        std::cerr
            << "Uso: topic_alive_monitor <topic> <timeout_s> [discovery_timeout_s]\n";
        return 1;
    }

    const std::string topic        = argv[1];
    double            timeout_s    = 0.0;
    double            disc_timeout = 5.0;

    try {
        timeout_s   = std::stod(argv[2]);
        if (argc >= 4) disc_timeout = std::stod(argv[3]);
    } catch (const std::exception & e) {
        std::cerr << "Error en argumentos numéricos: " << e.what() << '\n';
        return 1;
    }

    if (timeout_s <= 0.0) {
        std::cerr << "timeout_s debe ser > 0\n";
        return 1;
    }

    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    rclcpp::init(argc, argv);

    auto node = std::make_shared<TopicAliveMonitor>(
        topic, timeout_s, disc_timeout);

    rclcpp::executors::SingleThreadedExecutor executor;
    executor.add_node(node);

    // Bucle principal: spin hasta señal externa o evento terminal del nodo
    while (!g_shutdown && rclcpp::ok()) {
        executor.spin_some(std::chrono::milliseconds(50));
    }

    // Emitir shutdown antes de destruir el nodo
    double final_elapsed = 0.0;
    try {
        // node puede ya no tener clock válido si rclcpp se está apagando
        final_elapsed = (node->now() - node->get_clock()->now()).seconds();
    } catch (...) {}

    std::cout << json_event("shutdown", topic, final_elapsed) << '\n';
    std::cout.flush();

    rclcpp::shutdown();
    return 0;
}
