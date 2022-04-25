    def __send2api__(self, msg):
        msg = bytes(msg).encode("utf-8")
        requests.post(API_ENDPOINT, msg)
