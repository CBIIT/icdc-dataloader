
def check_encoding(file_name):
    utf8 = 'utf-8'
    windows1252 = 'windows-1252'
    try:
        with open(file_name, encoding=utf8) as file:
            for _ in file.readlines():
                pass
        return utf8
    except UnicodeDecodeError:
        return windows1252
    