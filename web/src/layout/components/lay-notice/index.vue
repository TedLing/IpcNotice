<script setup lang="ts">
// 【消息提醒模块已整体停用】右上角小铃铛不再出现。
// 1) lay-navbar/index.vue、lay-sidebar/NavMix.vue、lay-sidebar/NavHorizontal.vue
//    中的 LayNotice import 与 <LayNotice> 渲染均已注释；
// 2) ./data.ts 中的模拟「通知 / 消息 / 待办」数据已整体注释，noticesData 为空数组；
// 3) 下方原本的数据加载与未读角标计数逻辑已注释。
// 恢复方式：取消三处引用注释 + 恢复 data.ts 数据数组 + 取消下方计数逻辑注释。
import { ref, computed } from "vue";
import type { TabItem } from "./data";
import NoticeList from "./components/NoticeList.vue";
import BellIcon from "~icons/ep/bell";

// import { noticesData } from "./data";
// const notices = ref(noticesData);
// const activeKey = ref(noticesData[0]?.key);
// notices.value.map(v => (noticesNum.value += v.list.length));

// 停用后的占位：无任何消息、未读角标恒为 0；组件本身仍可编译，便于随时恢复。
const noticesNum = ref(0);
const notices = ref<TabItem[]>([]);
const activeKey = ref("");

const getLabel = computed(
  () => item =>
    item.name + (item.list.length > 0 ? `(${item.list.length})` : "")
);
</script>

<template>
  <el-dropdown trigger="click" placement="bottom-end">
    <span
      :class="[
        'dropdown-badge',
        'navbar-bg-hover',
        'select-none',
        Number(noticesNum) !== 0 && 'mr-[10px]'
      ]"
    >
      <el-badge :value="Number(noticesNum) === 0 ? '' : noticesNum" :max="99">
        <span class="header-notice-icon">
          <IconifyIconOffline :icon="BellIcon" />
        </span>
      </el-badge>
    </span>
    <template #dropdown>
      <el-dropdown-menu>
        <el-tabs
          v-model="activeKey"
          :stretch="true"
          class="dropdown-tabs"
          :style="{ width: notices.length === 0 ? '200px' : '330px' }"
        >
          <el-empty
            v-if="notices.length === 0"
            description="暂无消息"
            :image-size="60"
          />
          <span v-else>
            <template v-for="item in notices" :key="item.key">
              <el-tab-pane :label="getLabel(item)" :name="`${item.key}`">
                <el-scrollbar max-height="330px">
                  <div class="noticeList-container">
                    <NoticeList :list="item.list" :emptyText="item.emptyText" />
                  </div>
                </el-scrollbar>
              </el-tab-pane>
            </template>
          </span>
        </el-tabs>
      </el-dropdown-menu>
    </template>
  </el-dropdown>
</template>

<style lang="scss" scoped>
.dropdown-badge {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 40px;
  height: 48px;
  cursor: pointer;

  .header-notice-icon {
    font-size: 18px;
  }
}

.dropdown-tabs {
  .noticeList-container {
    padding: 15px 24px 0;
  }

  :deep(.el-tabs__header) {
    margin: 0;
  }

  :deep(.el-tabs__nav-wrap)::after {
    height: 1px;
  }

  :deep(.el-tabs__nav-wrap) {
    padding: 0 36px;
  }
}
</style>
