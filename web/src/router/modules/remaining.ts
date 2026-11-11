const Layout = () => import("@/layout/index.vue");

export default [
  {
    path: "/login",
    name: "Login",
    component: () => import("@/views/login/index.vue"),
    meta: {
      title: "登录",
      showLink: false
    }
  },
  // 全屏403（无权访问）页面
  {
    path: "/access-denied",
    name: "AccessDenied",
    component: () => import("@/views/error/403.vue"),
    meta: {
      title: "403",
      showLink: false
    }
  },
  // 全屏500（服务器出错）页面
  {
    path: "/server-error",
    name: "ServerError",
    component: () => import("@/views/error/500.vue"),
    meta: {
      title: "500",
      showLink: false
    }
  },
  // 「异常页面」菜单模块已移除；以下隐藏路由仅用于框架内部跳转
  // （router/index.ts 的权限校验与 VITE_HIDE_HOME 会跳到 /error/403、/error/404）。
  {
    path: "/error/403",
    name: "Error403",
    component: () => import("@/views/error/403.vue"),
    meta: { title: "403", showLink: false }
  },
  {
    path: "/error/404",
    name: "Error404",
    component: () => import("@/views/error/404.vue"),
    meta: { title: "404", showLink: false }
  },
  {
    path: "/error/500",
    name: "Error500",
    component: () => import("@/views/error/500.vue"),
    meta: { title: "500", showLink: false }
  },
  {
    path: "/redirect",
    component: Layout,
    meta: {
      title: "加载中...",
      showLink: false
    },
    children: [
      {
        path: "/redirect/:path(.*)",
        name: "Redirect",
        component: () => import("@/layout/redirect.vue")
      }
    ]
  }
] satisfies Array<RouteConfigsTable>;
