// IpcNotice 业务菜单。顶层分组会被 pure-admin 自动拍平到主 Layout 下，
// 菜单渲染仍保留这里的二级结构。
export default [
  {
    path: "/device",
    name: "DeviceManage",
    meta: {
      title: "设备管理",
      icon: "ep/video-camera",
      rank: 1
    },
    children: [
      {
        path: "/device/camera",
        name: "DeviceCamera",
        component: () => import("@/views/device/camera.vue"),
        meta: { title: "摄像头管理" }
      },
      {
        path: "/device/subscribe",
        name: "DeviceSubscribe",
        component: () => import("@/views/device/subscribe.vue"),
        meta: { title: "事件订阅" }
      }
    ]
  },
  {
    path: "/config",
    name: "IpcConfig",
    meta: {
      title: "配置中心",
      icon: "ep/setting",
      rank: 2
    },
    children: [
      {
        path: "/config/channel",
        name: "ConfigChannel",
        component: () => import("@/views/config/channel.vue"),
        meta: { title: "Webhook 通道" }
      },
      {
        path: "/config/settings",
        name: "ConfigSettings",
        component: () => import("@/views/config/settings.vue"),
        meta: { title: "系统设置" }
      }
    ]
  },
  {
    path: "/monitor",
    name: "IpcMonitor",
    meta: {
      title: "运行监控",
      icon: "ep/data-line",
      rank: 3
    },
    children: [
      {
        path: "/monitor/events",
        name: "MonitorEvents",
        component: () => import("@/views/monitor/events.vue"),
        meta: { title: "事件日志" }
      },
      {
        path: "/monitor/runtime",
        name: "MonitorRuntime",
        component: () => import("@/views/monitor/runtime.vue"),
        meta: { title: "运行态" }
      }
    ]
  }
] satisfies Array<RouteConfigsTable>;
