"""MQTT-клиент для открывания домофона Интерсвязь.

У Интерсвязь нет собственного MQTT-брокера — MQTT здесь используется как
интерфейс команд: клиент подписывается на топик, и при получении любого
сообщения открывает дверь через REST API (api_client.py). Это удобно для
интеграции с Home Assistant / Node-RED / любой другой системой, умеющей
публиковать в MQTT.

Авторизация выполняется по SMS. Токен сохраняется в файл и переиспользуется
при последующих запусках, повторный SMS-логин происходит только если
сохранённый токен истёк.

Все настройки берутся из YAML-файла (см. config.example.yaml).
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any, Optional

import aiohttp
import aiomqtt
import yaml

from api_client import (
    IntersvyazApiClient,
    IntersvyazApiError,
    IntersvyazAuthError,
)

_LOGGER = logging.getLogger("intersvyaz_mqtt")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)


# --------------------------------------------------------------------- #
# Конфигурация из YAML
# --------------------------------------------------------------------- #
class Config:
    def __init__(self, path: str) -> None:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}

        isv = raw.get("intersvyaz") or {}
        if "phone" not in isv:
            raise ValueError("В конфиге не указан intersvyaz.phone")
        self.phone: str = str(isv["phone"])
        self.relay_id: Optional[str] = isv.get("relay_id") or None
        self.token_file: str = isv.get("token_file", "intersvyaz_token.json")

        mqtt = raw.get("mqtt") or {}
        self.mqtt_host: str = mqtt.get("host", "localhost")
        self.mqtt_port: int = int(mqtt.get("port", 1883))
        self.mqtt_username: Optional[str] = mqtt.get("username")
        self.mqtt_password: Optional[str] = mqtt.get("password")
        self.base_topic: str = mqtt.get("base_topic", "intersvyaz/domofon").rstrip("/")

    @property
    def topic_open(self) -> str:
        return f"{self.base_topic}/open/set"

    @property
    def topic_state(self) -> str:
        return f"{self.base_topic}/state"

    @property
    def topic_relays(self) -> str:
        return f"{self.base_topic}/relays"

    @property
    def topic_result(self) -> str:
        return f"{self.base_topic}/result"

    @property
    def topic_cameras(self) -> str:
        return f"{self.base_topic}/cameras"


# --------------------------------------------------------------------- #
# Сохранение токена между запусками
# --------------------------------------------------------------------- #
class TokenStorage:
    def __init__(self, path: str) -> None:
        self._path = Path(path)

    def load(self) -> Optional[str]:
        if not self._path.exists():
            return None
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            return data.get("token")
        except (ValueError, OSError) as err:
            _LOGGER.warning("Не удалось прочитать файл токена %s: %s", self._path, err)
            return None

    def save(self, token: str) -> None:
        try:
            self._path.write_text(json.dumps({"token": token}), encoding="utf-8")
            _LOGGER.debug("Токен сохранён в %s", self._path)
        except OSError as err:
            _LOGGER.warning("Не удалось сохранить токен в %s: %s", self._path, err)


# --------------------------------------------------------------------- #
# Авторизация (сохранённый токен -> при необходимости SMS-логин)
# --------------------------------------------------------------------- #
async def _ask_input(prompt: str) -> str:
    """Неблокирующий input(): выполняется в отдельном потоке."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, input, prompt)


async def authenticate(
    client: IntersvyazApiClient, config: Config, storage: TokenStorage
) -> None:
    """Использует сохранённый токен либо запускает вход по SMS."""
    token = storage.load()
    if token:
        client.token = token
        try:
            await client.get_relays()
            _LOGGER.info("Авторизация по сохранённому токену прошла успешно")
            return
        except IntersvyazAuthError:
            _LOGGER.info("Сохранённый токен недействителен, вход по SMS")

    device_id = await client.send_sms(config.phone)
    _LOGGER.info("SMS с кодом отправлена на %s", config.phone)
    code = await _ask_input("Введите код из SMS: ")
    confirm = await client.confirm_sms(config.phone, code.strip(), device_id)

    addresses = confirm.get("addresses") or []
    if not addresses:
        raise IntersvyazApiError("В ответе сервера нет ни одного адреса/лицевого счёта")

    if len(addresses) > 1:
        print("Найдено несколько адресов/лицевых счетов:")
        for i, addr in enumerate(addresses):
            print(f"  [{i}] {addr}")
        idx = await _ask_input("Выберите номер адреса: ")
        address = addresses[int(idx.strip())]
    else:
        address = addresses[0]

    user_id = address.get("USER_ID") or address.get("userId") or address.get("id")
    if not user_id:
        raise IntersvyazApiError(f"Не удалось определить USER_ID в ответе: {address}")

    token = await client.token_by_address(confirm["authId"], user_id)
    storage.save(token)
    _LOGGER.info("Авторизация по SMS прошла успешно, токен сохранён")


# --------------------------------------------------------------------- #
# Определение relay_id
# --------------------------------------------------------------------- #
async def resolve_relay_id(
    client: IntersvyazApiClient,
    config: Config,
    mqtt_client: Optional["aiomqtt.Client"] = None,
) -> str:
    """Если relay_id не задан в конфиге — печатает список реле и завершает работу."""
    if config.relay_id:
        return config.relay_id

    relays = await client.get_relays()
    if not relays:
        raise IntersvyazApiError("API не вернул ни одного реле")

    print("relay_id не указан в конфигурации. Доступные реле:")
    for r in relays:
        print(f"  RELAY_ID={r.get('RELAY_ID')}  NAME={r.get('RELAY_TYPE', '?')}")
    print(
        "Укажите нужный RELAY_ID в поле intersvyaz.relay_id конфигурационного "
        "файла и перезапустите клиент."
    )

    if mqtt_client is not None:
        try:
            await mqtt_client.publish(
                config.topic_relays, json.dumps(relays, ensure_ascii=False)
            )
        except aiomqtt.MqttError as err:
            _LOGGER.debug("Не удалось опубликовать список реле в MQTT: %s", err)

    raise SystemExit(1)


# --------------------------------------------------------------------- #
# Основной цикл: подписка на топик открытия, обработка команд
# --------------------------------------------------------------------- #
async def run(config_path: str) -> None:
    config = Config(config_path)
    storage = TokenStorage(config.token_file)

    async with aiohttp.ClientSession() as session:
        api_client = IntersvyazApiClient(session)
        await authenticate(api_client, config, storage)

        relay_id = config.relay_id or await resolve_relay_id(api_client, config)

        cameras = await api_client.get_all_cameras()
        camera_state = []
        for camera_item in cameras:
            camera = camera_item.get("camera")
            if camera is None:
                camera_state.append(camera_item)
                continue

            camera_media = camera.get("MEDIA") or {}
            snapshot = camera_media.get("SNAPSHOT") or {}
            snapshot_live = snapshot.get("LIVE") or {}
            hls = camera_media.get("HLS") or {}
            live = hls.get("LIVE") or {}
            camera_state.append(
                {
                    **{key: value for key, value in camera_item.items() if key != "camera"},
                    "uuid": camera.get("UUID"),
                    "id": camera.get("ID"),
                    "name": camera.get("NAME"),
                    "snapshot": {
                        "main": snapshot_live.get("MAIN"),
                        "LOSSY": snapshot_live.get("LOSSY"),
                    },
                    "hls": {
                        "main": live.get("MAIN"),
                        "low_latency": live.get("LOW_LATENCY"),
                    },
                }
            )
        _LOGGER.info("Загружены данные %s камер", len(camera_state))

        mqtt_kwargs: dict[str, Any] = {
            "hostname": config.mqtt_host,
            "port": config.mqtt_port,
        }
        if config.mqtt_username:
            mqtt_kwargs["username"] = config.mqtt_username
            mqtt_kwargs["password"] = config.mqtt_password

        _LOGGER.info(
            "Подключение к MQTT-брокеру %s:%s (relay_id=%s)",
            config.mqtt_host,
            config.mqtt_port,
            relay_id,
        )

        while True:
            try:
                async with aiomqtt.Client(**mqtt_kwargs) as mqtt_client:
                    await mqtt_client.publish(config.topic_state, "online", retain=True)
                    await mqtt_client.publish(
                        config.topic_cameras,
                        json.dumps(camera_state, ensure_ascii=False),
                    )
                    await mqtt_client.subscribe(config.topic_open)
                    _LOGGER.info(
                        "Подписан на топик %s, жду команды открытия", config.topic_open
                    )

                    async for _message in mqtt_client.messages:
                        _LOGGER.info("Получена команда открытия (relay_id=%s)", relay_id)
                        try:
                            await api_client.open_door(relay_id)
                            _LOGGER.info("Дверь открыта")
                            await mqtt_client.publish(config.topic_result, "OK")
                        except IntersvyazAuthError:
                            _LOGGER.warning("Токен истёк, повторная авторизация по SMS")
                            await authenticate(api_client, config, storage)
                            await api_client.open_door(relay_id)
                            await mqtt_client.publish(config.topic_result, "OK")
                        except IntersvyazApiError as err:
                            _LOGGER.error("Не удалось открыть дверь: %s", err)
                            await mqtt_client.publish(
                                config.topic_result, f"ERROR: {err}"
                            )

            except aiomqtt.MqttError as err:
                _LOGGER.warning(
                    "Соединение с MQTT-брокером потеряно (%s), переподключение через 5с",
                    err,
                )
                await asyncio.sleep(5)


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    try:
        asyncio.run(run(config_path))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
