import Axios, {
  type AxiosInstance,
  type AxiosRequestConfig,
  type CustomParamsSerializer
} from "axios";
import type {
  PureHttpError,
  PureHttpRequestConfig,
  PureHttpResponse,
  RequestMethods
} from "./types.d";
import { stringify } from "qs";
import { removeToken } from "@/utils/auth";

// 后端使用 HttpOnly 会话 Cookie 鉴权，前端不再维护 JWT 刷新逻辑。
const defaultConfig: AxiosRequestConfig = {
  timeout: 90000,
  withCredentials: true,
  headers: {
    Accept: "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest"
  },
  paramsSerializer: {
    serialize: stringify as unknown as CustomParamsSerializer
  }
};

class PureHttp {
  private static axiosInstance: AxiosInstance = Axios.create(defaultConfig);

  constructor() {
    this.httpInterceptorsRequest();
    this.httpInterceptorsResponse();
  }

  private httpInterceptorsRequest(): void {
    PureHttp.axiosInstance.interceptors.request.use(
      (config: PureHttpRequestConfig): any => {
        if (typeof config.beforeRequestCallback === "function") {
          config.beforeRequestCallback(config);
        }
        return config;
      },
      error => Promise.reject(error)
    );
  }

  private httpInterceptorsResponse(): void {
    PureHttp.axiosInstance.interceptors.response.use(
      (response: PureHttpResponse) => {
        const $config = response.config;
        if (typeof $config.beforeResponseCallback === "function") {
          $config.beforeResponseCallback(response);
        }
        return response.data;
      },
      (error: PureHttpError) => {
        const e = error as any;
        e.isCancelRequest = Axios.isCancel(error);
        const status = e?.response?.status;
        const url = e?.config?.url ?? "";
        if (status === 401 && !String(url).includes("/login")) {
          removeToken();
          if (!location.hash.startsWith("#/login")) {
            location.hash = "#/login";
            location.reload();
          }
        }
        const msg =
          e?.response?.data?.error ??
          e?.response?.data?.message ??
          e?.message ??
          "请求失败";
        return Promise.reject(new Error(msg));
      }
    );
  }

  public request<T>(
    method: RequestMethods,
    url: string,
    param?: AxiosRequestConfig,
    axiosConfig?: PureHttpRequestConfig
  ): Promise<T> {
    const config = {
      method,
      url,
      ...param,
      ...axiosConfig
    } as PureHttpRequestConfig;
    return PureHttp.axiosInstance.request(config) as unknown as Promise<T>;
  }

  public post<T, P>(
    url: string,
    params?: AxiosRequestConfig<P>,
    config?: PureHttpRequestConfig
  ): Promise<T> {
    return this.request<T>("post", url, params, config);
  }

  public get<T, P>(
    url: string,
    params?: AxiosRequestConfig<P>,
    config?: PureHttpRequestConfig
  ): Promise<T> {
    return this.request<T>("get", url, params, config);
  }
}

export const http = new PureHttp();
