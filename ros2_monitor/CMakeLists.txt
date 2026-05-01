cmake_minimum_required(VERSION 3.8)
project(ros2_monitor)

# ── Compiler flags ────────────────────────────────────────────────────────────
if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ── Dependencias ──────────────────────────────────────────────────────────────
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)

# ── Ejecutable ────────────────────────────────────────────────────────────────
add_executable(topic_alive_monitor topic_alive_monitor.cpp)

target_compile_features(topic_alive_monitor PUBLIC cxx_std_17)

ament_target_dependencies(topic_alive_monitor
  rclcpp
)

# ── Instalación ───────────────────────────────────────────────────────────────
install(TARGETS topic_alive_monitor
  DESTINATION lib/${PROJECT_NAME}
)

ament_package()
