"""
checkers/planning_first_lap_checker.py

Verifica que los mensajes de planning llegan (con un gap máximo de `threshold`
segundos) ÚNICAMENTE durante la primera vuelta, y que dejan de llegar en cuanto
el coche completa esa primera vuelta.

Usage in config.yaml:
    checkers:
      - type: PlanningFirstLapChecker
        planning_topic: /planning/trajectory
        lap_topic:      /car_state/car_info
        threshold:      1.0
"""

import time
import threading
import importlib

import rclpy
import rclpy.context
import rclpy.executors
from rclpy.node import Node

from .base_checker import BaseChecker

_TOPIC_DISCOVERY_TIMEOUT  = 30.0
_TOPIC_DISCOVERY_INTERVAL = 0.2


def _load_msg_class(type_str: str) -> type:
    parts = type_str.split("/")
    if len(parts) != 3:
        raise ValueError(f"Formato de tipo inesperado: '{type_str}'")
    pkg, _, cls_name = parts
    module = importlib.import_module(f"{pkg}.msg")
    return getattr(module, cls_name)


class PlanningFirstLapChecker(BaseChecker):

    def __init__(
        self,
        planning_topic: str,
        lap_topic: str,
        threshold: float,
        logger=None,
    ):
        super().__init__(name=f"PlanningFirstLapChecker({planning_topic})", logger=logger)

        self.planning_topic = planning_topic
        self.lap_topic      = lap_topic
        self.threshold      = threshold   # gap máximo permitido entre mensajes de planning

        # --- estado de vuelta ---
        self._current_lap      = 0        # actualizado por _lap_callback
        self._first_lap_done   = False    # True en cuanto lap_count sube de 0

        # --- estado de planning ---
        self._planning_received       = False   # ¿llegó algún msg de planning en lap 1?
        self._last_planning_time      = None    # timestamp del último msg de planning
        self._planning_after_lap1     = False   # ¿llegó algún msg tras la primera vuelta?

        # --- ROS ---
        self._context      = None
        self._node         = None
        self._executor     = None
        self._spin_thread  = None
        self._timer        = None

        # Un setup_thread por topic
        self._setup_planning_thread = None
        self._setup_lap_thread      = None

    # ── Startup ────────────────────────────────────────────────────────────

    def _on_start(self):
        self._context = rclpy.context.Context()
        rclpy.init(context=self._context)

        node_name = "planning_first_lap_" + self.planning_topic.replace("/", "_").strip("_")
        self._node = Node(node_name, context=self._context)

        self._executor = rclpy.executors.SingleThreadedExecutor(context=self._context)
        self._executor.add_node(self._node)

        self._spin_thread = threading.Thread(
            target=self._spin_safely, daemon=True
        )
        self._spin_thread.start()

        self._setup_planning_thread = threading.Thread(
            target=self._discover_and_subscribe,
            args=(self.planning_topic, self._planning_callback),
            daemon=True,
        )
        self._setup_planning_thread.start()

        self._setup_lap_thread = threading.Thread(
            target=self._discover_and_subscribe,
            args=(self.lap_topic, self._lap_callback),
            daemon=True,
        )
        self._setup_lap_thread.start()

        self.logger.info(f"[{self.name}] iniciado")

    # ── Discovery (genérico, reutilizable para ambos topics) ──────────────

    def _discover_and_subscribe(self, topic: str, callback):
        deadline = time.time() + _TOPIC_DISCOVERY_TIMEOUT

        while time.time() < deadline:
            if not self._running:
                return
            try:
                topics = dict(self._node.get_topic_names_and_types())
            except Exception:
                return

            if topic in topics:
                type_str = topics[topic][0]
                try:
                    msg_cls = _load_msg_class(type_str)
                    self._node.create_subscription(msg_cls, topic, callback, 10)
                    self.logger.info(f"[{self.name}] suscrito a '{topic}'")

                    # Arrancamos el timer de liveness solo tras suscribirnos al planning
                    if topic == self.planning_topic:
                        self._timer = self._node.create_timer(
                            self.threshold / 2.0, self._check_liveness
                        )
                    return
                except Exception as e:
                    self.logger.error(f"[{self.name}] Error cargando tipo de '{topic}': {e}")
                    return

            time.sleep(_TOPIC_DISCOVERY_INTERVAL)

        # Solo es fallo si no apareció el topic de planning; el de lap es informativo
        if topic == self.planning_topic:
            self._record_failure(
                f"Topic de planning '{topic}' no apareció en {_TOPIC_DISCOVERY_TIMEOUT}s."
            )

    def _spin_safely(self):
        try:
            self._executor.spin()
        except Exception:
            pass

    # ── Callbacks ─────────────────────────────────────────────────────────

    def _lap_callback(self, msg):
        """Actualiza el contador de vuelta. Ajusta el campo según el mensaje real."""
        lap = int(msg.lap_count)

        if lap != self._current_lap:
            self.logger.info(f"[{self.name}] Vuelta cambia {self._current_lap} → {lap}")
            self._current_lap = lap

        if lap >= 1 and not self._first_lap_done:
            self._first_lap_done = True
            self.logger.info(f"[{self.name}] Primera vuelta completada.")

    def _planning_callback(self, msg):
        now = time.time()

        if self._first_lap_done:
            # Llegó un mensaje de planning DESPUÉS de la primera vuelta → fallo
            if not self._planning_after_lap1:
                self._planning_after_lap1 = True
                self._record_failure(
                    f"Topic de planning '{self.planning_topic}' sigue publicando "
                    f"tras la primera vuelta (lap_count={self._current_lap})."
                )
            return

        # Estamos en la primera vuelta: comportamiento esperado
        self._last_planning_time = now
        if not self._planning_received:
            self._planning_received = True
            self.logger.info(f"[{self.name}] Primer mensaje de planning recibido.")

    # ── Timer de liveness (solo aplica durante la primera vuelta) ─────────

    def _check_liveness(self):
        """Dispara cada threshold/2 s para detectar silencios durante la primera vuelta."""
        if self._first_lap_done or not self._running:
            return
        if not self._planning_received or self._last_planning_time is None:
            return

        gap = time.time() - self._last_planning_time
        if gap > self.threshold:
            self._record_failure(
                f"Silencio en planning durante la primera vuelta: "
                f"{gap:.2f}s sin datos (umbral={self.threshold}s)."
            )

    # ── Shutdown ───────────────────────────────────────────────────────────

    def _on_stop(self):
        for t in (self._setup_planning_thread, self._setup_lap_thread):
            if t and t.is_alive():
                t.join(timeout=2.0)

        # ¿Llegó algo de planning durante la primera vuelta?
        if not self._planning_received and not self._failures:
            self._record_failure(
                f"Topic de planning '{self.planning_topic}': ningún mensaje "
                f"recibido durante la primera vuelta."
            )

        if self._executor:
            self._executor.shutdown(timeout_sec=2.0)
            self._executor = None
        if self._node:
            if self._timer:
                try:
                    self._timer.cancel()
                except Exception:
                    pass
            self._node.destroy_node()
            self._node = None
        if self._context:
            self._context.shutdown()
            self._context = None
        if self._spin_thread and self._spin_thread.is_alive():
            self._spin_thread.join(timeout=2.0)
