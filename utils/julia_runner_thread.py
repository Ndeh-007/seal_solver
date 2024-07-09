import os
import time
from uuid import uuid4

import numpy as np
from PySide6.QtCore import QThread, Signal

from utils.julia_helpers import SealSolver


class JuliaWorkerThread(QThread):
    stdout = Signal(object)
    stderr = Signal(object)
    threadFinished = Signal(str)
    threadStarted = Signal(str)

    def __init__(self, params: tuple[str, str], pid: str = None):
        super().__init__()
        self._params = params
        self.__pid = pid
        if pid is None:
            self.__pid = str(uuid4())

        self.finished.connect(self.__handleFinished)
        self.started.connect(self.__handleStarted)

    def __handleFinished(self):
        self.threadFinished.emit(self.__pid)

    def __handleStarted(self):
        self.threadStarted.emit(self.__pid)

    def pid(self):
        return self.__pid

    def run(self) -> None:
        try:
            in_dir, out_dir = self._params

            if not os.path.exists(in_dir):
                raise Exception(f"Path {in_dir} does not exists")

            if not os.path.exists(out_dir):
                raise Exception(f"Path {out_dir} does not exists")
            print((self.__pid, "Working..."))

            st = time.time()

            print((self.__pid, "Priming Solver"))

            res = SealSolver.solve(str(in_dir))
            et = time.time()
            t = et - st

            print((self.__pid, f"[STATUS]: Computation Completed - {t} secs"))
            print((self.__pid, "[STATUS]: Collecting Results"))

            DATA: dict[str, np.ndarray] = {
                "c_save": res[0],
                "t_save": res[1],
                "w_save": res[2],
                "v_save": res[3],
                "regime_save": res[4],
                "PSI_save": res[5],
                "p_save": res[6],
                "mesh_config": np.array([[res[7], res[8], res[9], t]])  # "zMD", "nphi", "nxi", "time"
            }

            # write the data to the various files in the results storage folder
            for key in DATA.keys():
                # create file path
                file = os.path.join(out_dir, f"{key}.npy")

                # get data
                data = DATA.get(key)

                # write data to path
                np.save(file, data)

            print((self.__pid, "[STATUS]: Results written to target files"))
            print((self.pid(), "action=completed"))

        except Exception as e:
            self.stderr.emit((self.__pid, e))

    # region event handlers

    # endregion
