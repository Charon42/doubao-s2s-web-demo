# 豆包端到端实时语音 SC2.0 切换说明

本文说明如何把当前项目从 O2.0 切换到 SC2.0。改动只需要配置 `.env`，不需要改前端播放器代码。

## 1. 当前默认模式

当前 `.env` 默认是 O2.0：

```env
VOICE_MODEL_TYPE=o2
VOICE_PROFILE=warm
```

O2.0 使用：

- `model = 1.2.1.1`
- `bot_name`
- `system_role`
- `speaking_style`
- O2.0 支持的 `zh_*_jupiter_bigtts` 音色

## 2. 切换到 SC2.0

打开 [backend/.env](backend/.env)，修改下面几项：

```env
VOICE_MODEL_TYPE=sc2
VOICE_PROFILE=warm
VOICE_SPEAKER=saturn_xxx
VOICE_CHARACTER_MANIFEST={"name":"小蔚","description":"一个自然、亲切、有情绪变化的中文语音助手。她像朋友一样和用户聊天，回答简洁，有真实聊天感。"}
```

其中：

- `VOICE_MODEL_TYPE=sc2` 表示启用 SC2.0。
- `VOICE_PROFILE` 可填 `warm`、`lively`、`calm`、`guide`，用于本地配置分组。
- `VOICE_SPEAKER` 必须换成你在豆包/火山控制台或文档里拿到的有效 SC2.0 音色，通常以 `saturn_` 或 `S_` 开头。
- `VOICE_CHARACTER_MANIFEST` 是 SC2.0 的角色设定，必须填写。

不要继续使用 O2.0 的 `zh_female_xxx_jupiter_bigtts` 音色，否则可能出现 `InvalidSpeaker`。

## 3. 可选：按 profile 配置多个 SC2.0 音色

如果你想保留多个风格，可以这样写：

```env
VOICE_MODEL_TYPE=sc2
VOICE_PROFILE=warm
VOICE_SPEAKER=

VOICE_SC2_SPEAKER_WARM=saturn_xxx_warm
VOICE_SC2_SPEAKER_LIVELY=saturn_xxx_lively
VOICE_SC2_SPEAKER_CALM=saturn_xxx_calm
VOICE_SC2_SPEAKER_GUIDE=saturn_xxx_guide
```

然后只改：

```env
VOICE_PROFILE=lively
```

即可切换到对应音色。

如果同时设置了 `VOICE_SPEAKER` 和 `VOICE_SC2_SPEAKER_WARM`，优先使用 `VOICE_SPEAKER`。

## 4. character_manifest 示例

可以先用单行 JSON，避免 `.env` 换行解析问题：

```env
VOICE_CHARACTER_MANIFEST={"name":"小蔚","description":"自然、亲切、有情绪变化的中文语音助手。语气像朋友聊天，不像客服，也不像播音员。句子短，少用书面语。"}
```

如果你的官方文档或控制台提供了更完整的 `character_manifest` 结构，按官方结构填写，但建议仍保持单行。

## 5. 保持不变的音频配置

切到 SC2.0 后，项目仍强制要求豆包返回 PCM：

```json
"tts": {
  "audio_config": {
    "channel": 1,
    "format": "pcm_s16le",
    "sample_rate": 24000,
    "bits": 16
  }
}
```

前端播放器仍按：

- `pcm_s16le`
- `24000Hz`
- `mono`
- `int16 little-endian`

处理音频。

之前修复过的 PCM 对齐、延迟 2 包播放、尾包合并、50ms fade out、播放队列 drain 都不需要改。

## 6. 重启服务

修改 `.env` 后，重启后端服务，让配置重新加载。

启动后看后端日志，应该能看到类似：

```text
voice_model_type: sc2
voice_profile: warm
model: 2.2.0.0
speaker: saturn_xxx
StartSession full JSON: ...
```

重点确认 `StartSession full JSON` 里：

```json
"extra": {
  "model": "2.2.0.0"
}
```

并且存在：

```json
"character_manifest": "..."
```

## 7. 常见错误

### InvalidSpeaker

通常是 `VOICE_SPEAKER` 和 `VOICE_MODEL_TYPE` 不匹配。

SC2.0 不要用：

```env
zh_female_xiaohe_jupiter_bigtts
zh_female_vv_jupiter_bigtts
zh_male_yunzhou_jupiter_bigtts
zh_male_xiaotian_jupiter_bigtts
```

请换成有效的 `saturn_` 或 `S_` 音色。

### 缺少 character_manifest

SC2.0 当前校验要求填写：

```env
VOICE_CHARACTER_MANIFEST=...
```

否则后端会拒绝启动会话。

### 听不到声音或格式错误

检查 `StartSession full JSON` 中是否仍有：

```json
"format": "pcm_s16le"
```

如果服务端返回 OGG Opus，当前播放器不会按 PCM 播放，会报格式错误。

## 8. 回滚到 O2.0

把 `.env` 改回：

```env
VOICE_MODEL_TYPE=o2
VOICE_PROFILE=warm
VOICE_SPEAKER=
VOICE_CHARACTER_MANIFEST=
```

然后重启后端。

O2.0 会自动使用：

```env
model=1.2.1.1
speaker=zh_female_xiaohe_jupiter_bigtts
```

并继续使用 `bot_name/system_role/speaking_style`。
