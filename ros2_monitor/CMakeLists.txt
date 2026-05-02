cmake_minimum_required(VERSION 3.10)
project(ros2_monitor)

# Compilar usando el estándar de C++17 (Requerido por ROS2 Humble)
if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

# 1. Buscar dependencias de ROS2
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)

# 2. Declarar el ejecutable (ahora solo con el .cpp, ya que el .hpp se incluye dentro)
add_executable(topic_alive_monitor topic_alive_monitor.cpp)

# 3. Indicarle a CMake dónde buscar los archivos de cabecera (.hpp)
# El "." significa "busca en este mismo directorio"
target_include_directories(topic_alive_monitor PRIVATE .)

# 4. Vincular las dependencias al ejecutable
ament_target_dependencies(topic_alive_monitor
  rclcpp
  std_msgs
)

# 5. Instalar el ejecutable en el espacio de trabajo
# Esto es vital para que tu script de Python pueda invocarlo luego
install(TARGETS
  topic_alive_monitor
  DESTINATION lib/${PROJECT_NAME}
)

ament_package()
