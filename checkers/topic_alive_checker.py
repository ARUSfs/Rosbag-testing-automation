import time
import threading
import importlib
import rclpy
import rclpy.executors
from rclpy.node import Node
from .base_checker import BaseChecker

def _load_msg_class(type_str: str) -> type:
    parts = type_str.split("/")
    if len(parts) != 3:
        raise ValueError(f"Formato de tipo inesperado: '{type_str}'")
    pkg, _, cls_name = parts
    module = importlib.import_module(f"{pkg}.msg")
    return getattr(module, cls_name)

class TopicAliveChecker(BaseChecker):

    def __init__(self, topic: str, seconds: int, logger=None):
        super().__init__(name=f"TopicAliveChecker({topic})", logger=logger)
        self.topic          = topic
        self.seconds        = seconds
        self.current_lap    = 0
        self._received      = False
        self._last_msg_time = None
        self._node          = None
        self._executor      = None
        self._spin_thread   = None
        self._setup_thread  = None
        self._timer         = None

    def _on_start(self):
        if not rclpy.ok():
            rclpy.init()

        node_name = "topic_alive_" + self.topic.replace("/", "_").strip("_")
        self._node = Node(node_name)
        self._executor = rclpy.executors.SingleThreadedExecutor()
        self._executor.add_node(self._node)

        self._spin_thread = threading.Thread(
            target=self._spin_safely,
            daemon=True,
            name=f"spin_{node_name}",
        )
        self._spin_thread.start()

        self._setup_thread = threading.Thread(
            target=self._discover_and_subscribe,
            daemon=True,
            name=f"setup_{node_name}",
        )
        self._setup_thread.start()
        self.logger.info(f"[{self.name}] iniciado — buscando '{self.topic}'...")

    def _discover_and_subscribe(self):
        deadline = time.time() + 30.0 # _TOPIC_DISCOVERY_TIMEOUT

        while time.time() < deadline and self._running:
            try:
                # Intentamos obtener la lista de topics
                topics = dict(self._node.get_topic_names_and_types())
            except Exception:
                # Si falla el contexto de ROS pero estamos cerrando, salimos sin error
                if not self._running:
                    return
                time.sleep(0.2)
                continue

            if self.topic in topics:
                type_str = topics[self.topic][0]
                try:
                    msg_cls = _load_msg_class(type_str)
                    self._node.create_subscription(msg_cls, self.topic, self._callback, 10)
                    self._timer = self._node.create_timer(self.seconds / 2.0, self._check_liveness)
                    self.logger.info(f"[{self.name}] suscrito a '{self.topic}'")
                    return
                except Exception as e:
                    self.logger.error(f"[{self.name}] Error: {e}")
                    return

            time.sleep(0.2)

        if self._running and not self._received:
            self._record_failure(f"Topic '{self.topic}': no apareció tras el timeout.")

    def _spin_safely(self):
        try:
            if self._executor:
                self._executor.spin()
        except Exception:
            pass 

    def _callback(self, msg):
            # 1. Actualizar tiempo para el heartbeat
            self._last_msg_time = time.time()
            
            # 2. Actualizar vueltas si el mensaje tiene el campo (usamos hasattr para seguridad)
            if hasattr(msg, 'lap_count'):
                self.current_lap = msg.lap_count

            if not self._received:
                self._received = True
                self.logger.info(f"[{self.name}] Primer mensaje recibido.")

    def _check_liveness(self):
        if not self._received or not self._running or self._last_msg_time is None:
            return

        elapsed = time.time() - self._last_msg_time
        if elapsed > self.seconds and (self.current_lap > 1 and self.topic != "/path_planning/trajectory2"):
            self._record_failure(f"Silencio en '{self.topic}': {elapsed:.2f}s sin datos.")
            if self._timer:
                self._timer.cancel()

    def _on_stop(self):
        """Lógica de limpieza llamada por el método público stop()."""
        # 1. Parar discovery
        if self._setup_thread and self._setup_thread.is_alive():
            self._setup_thread.join(timeout=1.0)

        # 2. Registrar si nunca llegó nada
        if not self._received and not self._failures:
            self._record_failure(f"Topic '{self.topic}': no se recibió ningún mensaje.")

        # 3. Shutdown de ROS
        if self._executor:
            self._executor.shutdown(timeout_sec=1.0)
        
        if self._node:
            # Cancelamos timer explícitamente por seguridad
            if self._timer:
                try: self._timer.cancel()
                except: pass
            self._node.destroy_node()

        if self._spin_thread and self._spin_thread.is_alive():
            self._spin_thread.join(timeout=1.0)
