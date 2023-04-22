class RecordReader:
    def __init__(self, input_file):
        self.input_file = input_file
        self.current_key = None
        self.current_value = None

    def read(self):
        with open(self.input_file, 'r') as f:
            for line in f.readlines():
                parts = line.strip().split('\t')
                key = parts[0]
                value = parts[-1]

                if self.current_key is not None and key != self.current_key:
                    yield self.current_key, self.current_value
                    self.current_value = None

                self.current_key = key
                if self.current_value is None:
                    self.current_value = []
                self.current_value.append(value)
        if self.current_key is not None:
            yield self.current_key, self.current_value
