from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

from common import SingleInstanceLock


class SingleInstanceLockTest(unittest.TestCase):
    def test_acquire_and_release(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bot.lock"
            lock = SingleInstanceLock(str(path))
            lock.acquire()
            self.assertEqual(path.read_text(encoding="utf-8"), str(os.getpid()))
            lock.release()
            self.assertFalse(path.exists())

    def test_running_pid_blocks_second_instance(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bot.lock"
            path.write_text(str(os.getpid()), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "running PID"):
                SingleInstanceLock(str(path)).acquire()

    def test_stale_pid_is_removed_automatically(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bot.lock"
            path.write_text("99999999", encoding="utf-8")
            lock = SingleInstanceLock(str(path))
            lock.acquire()
            self.assertEqual(path.read_text(encoding="utf-8"), str(os.getpid()))
            lock.release()

    def test_recent_incomplete_lock_is_not_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bot.lock"
            path.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "being initialized"):
                SingleInstanceLock(str(path)).acquire()

    def test_old_incomplete_lock_is_removed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bot.lock"
            path.write_text("", encoding="utf-8")
            old_time = time.time() - 10.0
            os.utime(path, (old_time, old_time))
            lock = SingleInstanceLock(str(path))
            lock.acquire()
            self.assertEqual(path.read_text(encoding="utf-8"), str(os.getpid()))
            lock.release()


if __name__ == "__main__":
    unittest.main()
