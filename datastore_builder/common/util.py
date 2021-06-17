import random
import string


def create_run_id() -> str:
    char_set = string.ascii_lowercase + string.digits
    number = 6
    return '{}-{}'.format(''.join(random.sample(char_set * number, number)),
                          ''.join(random.sample(char_set * number, number)))
