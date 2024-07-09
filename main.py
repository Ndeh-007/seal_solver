import os
import sys

from PySide6.QtCore import QThread, QObject, Signal, QCoreApplication, QIODeviceBase, Slot
from PySide6.QtNetwork import QLocalSocket


# from utils.julia_helpers import PythonCall
from utils.julia_runner_thread import JuliaWorkerThread


class Communicator(QObject):
    """
    creates a server and a socket to allow for data transfer across the main application and the solver using different dedicated pipes
    """
    sent = Signal(str)  # when data has been sent via
    received = Signal(dict)  # signal for when data has been received. emitted after the data has been processed.
    started = Signal()  # when the communicator is turned on
    stopped = Signal()  # when the communicator is turned off
    stderr = Signal(object)

    stdout = Signal(str)

    def __init__(self, serverName: str, parent: QObject = None):
        super().__init__(parent=parent)
        self.__serverName = serverName
        if serverName is None:
            self.__serverName = "JULIA_SERVER"

        self.__readSocket = QLocalSocket()
        self.__writeSocket = QLocalSocket()

        self.__readSocket.setServerName(serverName)
        self.__writeSocket.setServerName(serverName)

        self.__isActive = False

    # region - initialize
    def prime(self):
        """
        defines the pipes for the socker and the server
        """
        self.__readSocket.connectToServer(self.__serverName, QIODeviceBase.OpenModeFlag.ReadOnly)
        self.__writeSocket.connectToServer(self.__serverName, QIODeviceBase.OpenModeFlag.WriteOnly)

    def configure(self):
        """
        primes the components of the communicator
        """
        self.__writeSocket.errorOccurred.connect(self.__handleSocketErrors)
        self.__readSocket.errorOccurred.connect(self.__handleSocketErrors)

        self.__writeSocket.connected.connect(self.__handleWriteSocketConnected)
        self.__readSocket.connected.connect(self.__handleReadSocketConnected)

        self.__readSocket.readyRead.connect(self.__handleReadSocketReadReady)

    def start(self):
        """
        launches the communicator
        """
        # connect the signals to slots
        self.configure()

        # establish connections
        self.prime()

        # test connections
        state, message = self.__testConnections()
        if state:
            self.__isActive = True
        else:
            self.stderr.emit('Failed to start connector, Sockets not properly primed.\n' + message)

        # validate sockets
        state = self.__testSocketValidity()
        if not state:
            self.__isActive = False
            self.stderr.emit('Sockets Failed Validity Test')
        else:
            self.__isActive = True
            self.stdout.emit('Communicator Primed')
            self.started.emit()

    def stop(self):
        """
        disconnects the sockets from their respective servers and flags the communicator as inactive
        """
        self.__readSocket.disconnectFromServer()
        self.__writeSocket.disconnectFromServer()

        state, error = self.__testDisconnections()
        if state:
            self.stdout.emit("Sockets disconnected")
            self.stopped.emit()
            self.__isActive = False
        else:
            self.stderr.emit("Failed to disconnect sockets: \n", error)

    # endregion

    # region - workers

    def __testSocketValidity(self):
        """
        calls valid functions on the sockets to make sure they are ready for use
        """
        return self.__readSocket.isValid() and self.__writeSocket.isValid()

    def __testConnections(self) -> tuple[bool, str]:
        """
        checks to see if the sockets are functioning
        """
        m = ""
        s1 = self.__writeSocket.waitForConnected(1000)
        if not s1:
            m = f"Write Socket Failed to connect: [{str(self.__writeSocket.error())}] {str(self.__writeSocket.errorString())}"

        s2 = self.__readSocket.waitForConnected(1000)
        if not s2:
            m = m + "\n---\n" + f"Read Socket Failed to Connect: [{str(self.__readSocket.error())}] {str(self.__readSocket.errorString())}"

        return s1 and s2, m

    def __testDisconnections(self) -> tuple[bool, str]:
        """
        checks to see if the sockets are functioning
        """
        m = ""
        s1 = self.__writeSocket.waitForDisconnected(1000)
        if not s1:
            m = f"Write Socket Failed to disconnect: [{str(self.__writeSocket.error())}] {str(self.__writeSocket.errorString())}"

        s2 = self.__readSocket.waitForDisconnected(1000)
        if not s2:
            m = m + "\n---\n" + f"Read Socket Failed to disconnect: [{str(self.__readSocket.error())}] {str(self.__readSocket.errorString())}"

        return s1 and s2, m

    def send(self, data: str):
        """
        sends data via the socket to the main application and emits the 'sent' signal
        """
        data = data + "\n"
        msg = data.encode('utf-8')
        self.__writeSocket.write(msg)
        self.sent.emit(data)

    def __parseInput(self, data: str):
        """
        parses the data into a dictionary and returns it as a dictionary.
        data should be of the format "key1=value1;key2=value2".
        data is delimited by a ';'
        """
        res = {}
        if len(data) == 0:
            self.stdout.emit("[MSG]: empty string was passed, returning.")
            return res

        if not isinstance(data, str):
            self.stdout.emit(f"[MSG]: Data with type '{type(data)}' was passed instead of 'str'.")
            return res

        keyValuePairs = data.split(";")
        for pair in keyValuePairs:

            pair = pair.strip()

            if len(pair) == 0:
                continue

            if "=" not in pair:
                raise Exception(f'Poorly formed data; {pair} in {keyValuePairs}')

            key, value = pair.split("=")
            if "," in value:
                value = value.split(",")

            res.update({key: value})

        return res

    # endregion

    # region - event handlers

    def __handleSocketErrors(self, error):
        """
        collect the socket errors and dispatch them
        """
        self.stderr.emit(error)

    def __handleReadSocketReadReady(self):
        """
        reads data from the server and processes it
        """
        msg = self.__readSocket.readAll().data().decode()
        data = self.__parseInput(msg)
        self.received.emit(data)

    def __handleReadSocketConnected(self):
        self.send("read socket connected")

    def __handleWriteSocketConnected(self):
        """
        when the socket has been connected to the server.
        """
        self.send("write socket connected")

    # endregion

    # region setters

    # endregion

    # region getters
    def isActive(self):
        return self.__isActive
    # endregion


class JuliaSolver(QObject):
    def __init__(self, args: list):
        super().__init__()
        self.__isLive = True
        self.__threads: dict[str, QThread] = {}
        self.__communicator: Communicator = Communicator(args[0] if len(args) > 0 else None)
        self.__logData(text=f"[STATUS]: Server Instantiated with params \n {str(args)}")
        self.__worker: QObject | None = None
        self.__thread: QThread | None = None

        self.configure()

    # region prime server

    def configure(self):
        """
        prime the event listener
        """
        self.__communicator.stderr.connect(self.__handleCommunicatorError)
        self.__communicator.stdout.connect(self.__handleCommunicatorOutput)

        self.__communicator.started.connect(self.__handleCommunicatorStarted)
        self.__communicator.stopped.connect(self.__handleCommunicatorStopped)

        self.__communicator.sent.connect(self.__handleCommunicatorDataSent)
        self.__communicator.received.connect(self.__handleCommunicatorDataReceived)

    # endregion

    # region start

    def launch(self):
        """
        creates the listener to handle inputs
        """
        self.__communicator.start()

    # endregion

    # region event handlers

    def __handleCommunicatorDataReceived(self, data):
        self.__handleListenerInput(data)

    def __handleCommunicatorDataSent(self, data):
        pass

    def __handleCommunicatorStarted(self):
        pass

    def __handleCommunicatorStopped(self):
        pass

    def __handleCommunicatorOutput(self, msg):
        self.__logData(text=msg, log=True)

    def __handleCommunicatorError(self, error):
        raise Exception(error)

    def __handleListenerLog(self, text):
        """
        manages inputs from the console listener
        """
        self.__logData(text=text)

    def __handleListenerInput(self, data: dict[str, str]):
        """
        uses inputted data to create the worker threads
        """
        self.__logData(text=f"[ACTION]: Data Received\n\t {data}, \n\tstimulating run\n")

        actions = ["kill", "start"]
        action = str(data.get("action"))
        if str(action) not in actions:
            raise Exception(f"[ERROR] Invalid actions.\n\tExpected value in {str(actions)} got {str(action)}")

        if action == "kill":
            self.__killThread(data.get("pid"))

        if action == "start":
            args = data.get("args")

            if not os.path.exists(args[0]):
                raise Exception(f"Path {args[0]} does not exists")

            if not os.path.exists(args[1]):
                raise Exception(f"Path {args[1]} does not exists")

            self.__spawnThread(data.get("pid"), (args[0], args[1]))

    def __handleThreadFinished(self, pid: str):
        """
        thread finished
        """
        # PythonCall.GC.enable()
        self.__logData(text=f"thread with pid: <{pid}> finished successfully", log=True, thread_id=pid, failed=False)
        if pid not in self.__threads.keys():
            return
        t = self.__threads.pop(pid)
        t.deleteLater()

    def __handleThreadStarted(self, pid):
        """
        handle thread started
        """
        # PythonCall.GC.disable()
        self.__logData(text=f"thread with pid: <{pid}> started", thread_id=pid)

    def __handleThreadError(self, errorTuple: tuple[str, object]):
        """
        processes thread errors
        """
        # PythonCall.GC.enable()
        pid, e = errorTuple
        t = self.__threads.pop(pid)
        t.deleteLater()
        self.__logData(text=f"[ERROR] Thread with id: '{pid}' failed. Error below", thread_id=pid, failed=True)
        raise Exception(e)

        # todo: send data to the main application

    def __handleThreadOutput(self, opts: tuple[str, str]):
        """
        collects data, formats and sends to the main application
        """
        pid, msg = opts
        self.__logData(text=msg, thread_id=pid)

    # endregion

    # region workers
    def __logData(self, failed: bool = False, text: str = None, log: bool = True, thread_id: str = None):
        """
        interface for encoding and writing information to the console to be read by the parent application.
        data is written in the format 'arg1=value1;arg2=value2;...;arg_n=value_n0,...,value_nm;'
        """
        msg = ""

        def appendKeyValuePair(basePair: str, key: str, value: str | None):
            if value is None:
                value = "none"
            return basePair + f"{key}={value};"

        msg = appendKeyValuePair(msg, "failed", str(failed).lower())  # if the process failed
        msg = appendKeyValuePair(msg, "text", text)  # the message
        msg = appendKeyValuePair(msg, "log", str(log).lower())  # should the message be logged?
        msg = appendKeyValuePair(msg, "pid", thread_id)  # the thread who these messages belong to

        self.__communicator.send(msg)

    def __killThread(self, thread_id):
        """
        kills thread or raises error if fail or target thread does not exists
        """
        # print('terminating thread')
        # if self.__thread is not None:
        #     print('thread terminated')
        #     self.__thread.terminate()
        # else:
        #     print('thread is None')
        # return

        thread = self.__threads.get(thread_id)
        if thread is None:
            self.__logData(text=f"[WARNING] Thread with id '{thread_id}', does not exists", thread_id=thread_id)
            return

        t = self.__threads.pop(thread_id)
        t.terminate()
        self.__logData(text=f"[ACTION] Thread<{thread_id}> Terminated", thread_id=thread_id)

    def __spawnThread(self, thread_id: str, args: tuple[str, str]):
        self.__logData(text="[ACTION] Spawning Thread")

        if thread_id in self.__threads.keys():
            self.__logData(thread_id=thread_id,
                           text=f"[WARNING] Thread with id '{thread_id}' already exist. Original thread will be killed and overridden")
            self.__killThread(thread_id)

        # create thread
        thread = JuliaWorkerThread(args, thread_id)

        # connect slots
        thread.threadFinished.connect(self.__handleThreadFinished)
        thread.threadStarted.connect(self.__handleThreadStarted)
        thread.stderr.connect(self.__handleThreadError)
        thread.stdout.connect(self.__handleThreadOutput)

        # collect thread
        self.__threads.update({thread_id: thread})

        # launch thread
        thread.run()

    @Slot()
    def threadFinished(self):
        self.__logData(text='thread finished')

    def threadStarted(self):
        self.__logData(text='thread started')

    # endregion


if __name__ == "__main__":
    app = QCoreApplication(sys.argv)
    julia_solver = JuliaSolver(sys.argv[1:])
    julia_solver.launch()
    sys.exit(app.exec())

