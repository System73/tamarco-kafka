from collections import UserDict
from unittest import mock


class AsyncMock(mock.MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class AsyncDict(UserDict):
    async def get(self, *args, **kwargs):
        return super().get(*args, **kwargs)
