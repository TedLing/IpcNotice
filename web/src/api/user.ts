import { http } from "@/utils/http";

export type UserResult = {
  success: boolean;
  data: {
    avatar: string;
    username: string;
    nickname: string;
    roles: Array<string>;
    permissions: Array<string>;
    accessToken: string;
    refreshToken: string;
    expires: Date;
  };
};

export type RefreshTokenResult = {
  success: boolean;
  data: {
    accessToken: string;
    refreshToken: string;
    expires: Date;
  };
};

/** 登录（后端写入 HttpOnly 会话 Cookie） */
export const getLogin = (data?: object) => {
  return http.request<UserResult>("post", "/api/login", { data });
};

/** 刷新会话（后端校验 Cookie 后返回一个新的前端占位 token） */
export const refreshTokenApi = (data?: object) => {
  return http.request<RefreshTokenResult>("post", "/api/refresh-token", {
    data
  });
};
