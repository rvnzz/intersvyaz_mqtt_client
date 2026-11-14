"""Единый асинхронный клиент для API Интерсвязь.

Раньше запросы к api.is74.ru и cams.is74.ru были продублированы в
__init__.py и в config_flow.py (с разным, местами несовместимым разбором
ответа — например get_relay_id в двух файлах ожидал разные форматы JSON).
Этот модуль — единственное место, где происходит обращение к сети, что
упрощает сопровождение и тестирование.
"""
from __future__ import annotations

import logging
import uuid as uuid_lib
from typing import Any, Optional

import aiohttp

_LOGGER = logging.getLogger(__name__)

BASE_URL = "https://api.is74.ru"
BASE_URL_CAM = "https://cams.is74.ru"
BASE_URL_CDN = "https://cdn.cams.is74.ru"

API_TIMEOUT = aiohttp.ClientTimeout(total=15)


class IntersvyazApiError(Exception):
    """Базовая ошибка при обращении к API Интерсвязь."""


class IntersvyazAuthError(IntersvyazApiError):
    """Токен недействителен/истёк или неверные учётные данные."""


class IntersvyazApiClient:
    """Тонкая обёртка над мобильным API Интерсвязь."""

    def __init__(self, session: aiohttp.ClientSession, token: Optional[str] = None) -> None:
        self._session = session
        self.token = token

    def _auth_headers(self) -> dict[str, str]:
        if not self.token:
            raise IntersvyazAuthError("Токен не установлен")
        return {"Authorization": f"Bearer {self.token}"}

    async def _request(
        self, method: str, url: str, *, auth: bool = False, **kwargs: Any
    ) -> Any:
        headers = kwargs.pop("headers", {}) or {}
        if auth:
            headers.update(self._auth_headers())

        try:
            async with self._session.request(
                method, url, headers=headers, timeout=API_TIMEOUT, **kwargs
            ) as resp:
                text = await resp.text()

                if resp.status in (401, 403):
                    raise IntersvyazAuthError(
                        f"Ошибка авторизации ({resp.status}) при запросе {url}"
                    )
                if resp.status != 200:
                    _LOGGER.debug("Ответ %s на %s: %s", resp.status, url, text[:500])
                    raise IntersvyazApiError(f"HTTP {resp.status} при запросе {url}")

                if not text:
                    return None
                try:
                    return await resp.json(content_type=None)
                except ValueError as err:
                    raise IntersvyazApiError(
                        f"Не удалось разобрать ответ сервера ({url}): {err}"
                    ) from err
        except aiohttp.ClientError as err:
            raise IntersvyazApiError(f"Ошибка сети при запросе {url}: {err}") from err
        except TimeoutError as err:
            raise IntersvyazApiError(f"Таймаут при запросе {url}") from err

    # ------------------------------------------------------------------ #
    # Авторизация по логину/паролю
    # ------------------------------------------------------------------ #
    async def login(self, username: str, password: str) -> str:
        data = await self._request(
            "POST",
            f"{BASE_URL}/auth/mobile",
            json={"username": username, "password": password},
            headers={"Content-Type": "application/json"},
        )
        token = data.get("TOKEN") if data else None
        if not token:
            raise IntersvyazAuthError("Токен отсутствует в ответе сервера")
        self.token = token
        return token

    # ------------------------------------------------------------------ #
    # Авторизация по номеру телефона (SMS)
    # ------------------------------------------------------------------ #
    async def send_sms(self, phone: str) -> str:
        """Отправляет СМС с кодом, возвращает device_id для следующих шагов."""
        device_id = uuid_lib.uuid4().hex
        await self._request(
            "POST",
            f"{BASE_URL}/mobile/auth/send-sms",
            json={"phone": phone.replace("+7", ""), "uniqueDeviceId": device_id},
        )
        return device_id

    async def confirm_sms(self, phone: str, code: str, device_id: str) -> dict:
        data = await self._request(
            "POST",
            f"{BASE_URL}/mobile/auth/confirm",
            json={
                "confirmCode": code,
                "phone": phone.replace("+7", ""),
                "uniqueDeviceId": device_id,
            },
        )
        if not data or "authId" not in data or "addresses" not in data:
            raise IntersvyazApiError("Неверный код подтверждения")
        return data

    async def token_by_address(self, auth_id: str, user_id: str) -> str:
        data = await self._request(
            "POST",
            f"{BASE_URL}/mobile/auth/get-token",
            json={"authId": str(auth_id).strip(), "userId": str(user_id).strip()},
            headers={"Content-Type": "application/json"},
        )
        token = (data.get("TOKEN") or data.get("token")) if data else None
        if not token:
            raise IntersvyazAuthError("Токен отсутствует в ответе сервера")
        self.token = token
        return token

    # ------------------------------------------------------------------ #
    # Домофон / реле
    # ------------------------------------------------------------------ #
    async def get_relays(self) -> list[dict]:
        """Возвращает список ВСЕХ доступных реле (для выбора нужного relay_id)."""
        data = await self._request("GET", f"{BASE_URL}/domofon/relays", auth=True)
        if isinstance(data, list):
            return data
        raise IntersvyazApiError("API не вернул список реле")

    async def get_relay_id(self) -> str:
        """Оставлено для обратной совместимости: возвращает ID первого реле."""
        relays = await self.get_relays()
        if relays:
            relay_id = relays[0].get("RELAY_ID")
            if relay_id:
                return relay_id
        raise IntersvyazApiError("API не вернул список реле")

    async def open_door(self, relay_id: str) -> None:
        await self._request(
            "POST",
            f"{BASE_URL}/domofon/relays/{relay_id}/open?from=app",
            auth=True,
            headers={"Content-Type": "application/json"},
        )

    # ------------------------------------------------------------------ #
    # Камеры
    # ------------------------------------------------------------------ #
    async def get_group_id(self) -> list[str]:
        """Возвращает ID всех групп камер, кроме 'Улицы Онлайн'."""
        data = await self._request("GET", f"{BASE_URL_CAM}/api/get-group/", auth=True)
        if not isinstance(data, list):
            raise IntersvyazApiError("API не вернул список групп камер")

        return [
            str(group["ID"])
            for group in data
            if isinstance(group, dict)
            and group.get("NAME") != "Улицы Онлайн"
            and group.get("ID") is not None
        ]

    async def get_group_cameras(self, group_id: str) -> list[dict]:
        data = await self._request(
            "GET", f"{BASE_URL_CAM}/api/get-group/{group_id}", auth=True
        )
        if not isinstance(data, list):
            raise IntersvyazApiError(f"Неожиданный формат данных камер: {type(data)}")
        return data

    async def get_camera_by_uuid(self, camera_uuid: str) -> dict:
        """Получает метаданные камеры по UUID, включая доступные URL потоков."""
        data = await self._request(
            "POST",
            f"{BASE_URL_CAM}/api/limited-info-by-uuid",
            auth=True,
            data={"CAMERA_UUIDS[]": camera_uuid},
        )
        if not isinstance(data, dict):
            raise IntersvyazApiError("API не вернул данные камеры по UUID")

        camera = data.get(camera_uuid)
        if camera is None:
            camera = next(
                (
                    item
                    for item in data.values()
                    if isinstance(item, dict)
                    and str(item.get("UUID", "")).lower() == camera_uuid.lower()
                ),
                None,
            )
        if camera is None and len(data) == 1:
            candidate = next(iter(data.values()))
            if isinstance(candidate, dict) and candidate.get("UUID"):
                camera = candidate
        if not isinstance(camera, dict):
            raise IntersvyazApiError(
                f"Камера с UUID {camera_uuid} не найдена в ответе API"
            )
        return camera

    async def get_intercom_cameras(self) -> list[dict]:
        """Загружает камеры всех реле, для которых API заявляет наличие видео."""
        cameras: list[dict] = []
        for relay in await self.get_relays():
            if str(relay.get("HAS_VIDEO", "0")) != "1":
                continue

            relay_id = relay.get("RELAY_ID")
            camera_uuid = relay.get("ENTRANCE_UID")
            if not relay_id or not camera_uuid:
                _LOGGER.warning("Пропущено видеореле без RELAY_ID или ENTRANCE_UID: %s", relay)
                continue

            try:
                camera = await self.get_camera_by_uuid(str(camera_uuid))
            except IntersvyazApiError as err:
                _LOGGER.warning(
                    "Не удалось загрузить камеру видеореле %s: %s", relay_id, err
                )
                cameras.append(
                    {
                        "relay_id": str(relay_id),
                        "entrance_uuid": str(camera_uuid),
                        "relay_name": relay.get("RELAY_DESCR") or relay.get("ADDRESS"),
                        "error": str(err),
                    }
                )
                continue

            cameras.append(
                {
                    "relay_id": str(relay_id),
                    "entrance_uuid": str(camera_uuid),
                    "relay_name": relay.get("RELAY_DESCR") or relay.get("ADDRESS"),
                    "camera": camera,
                }
            )
        return cameras

    async def get_all_cameras(self) -> list[dict]:
        """Объединяет камеры домофонов и камеры всех пользовательских групп."""
        cameras = [
            {"source": "intercom", **item}
            for item in await self.get_intercom_cameras()
        ]
        group_ids = await self.get_group_id()
        for group_id in group_ids:
            try:
                group_cameras = await self.get_group_cameras(group_id)
            except IntersvyazApiError as err:
                _LOGGER.warning(
                    "Не удалось загрузить камеры группы %s: %s", group_id, err
                )
                continue
            cameras.extend(
                {"source": "yard", "group_id": group_id, "camera": camera}
                for camera in group_cameras
            )
        return cameras

    def snapshot_url(self, camera_uuid: str) -> str:
        return f"{BASE_URL_CDN}/snapshot?uuid={camera_uuid}&token=bearer-{self.token}"

    def fallback_stream_url(self, camera_uuid: str) -> str:
        return (
            f"{BASE_URL_CDN}/hls/playlists/multivariant.m3u8"
            f"?uuid={camera_uuid}&token=bearer-{self.token}"
        )
