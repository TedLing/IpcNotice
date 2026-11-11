// 本项目全部菜单为静态路由（src/router/modules/*.ts），不依赖后端下发动态路由。
// 返回空数组即可让 pure-admin 用 constantMenus 初始化菜单，
// 这样打包成 dist 由 Go 静态托管时也能正常登录，无需 mock 服务。
type Result = {
  success: boolean;
  data: Array<any>;
};

export const getAsyncRoutes = (): Promise<Result> => {
  return Promise.resolve({ success: true, data: [] });
};
