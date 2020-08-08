from subprocess import call
import threading


def handler(index):
    text = "hello" + str(index)
    call(["Python3", "s3_operations.py", "upload", "--name", "xebrium12345", "--text", text])


if __name__ == "__main__":
    for i in range(5):
        t = threading.Thread(target=handler, args=[i])
        t.start()
