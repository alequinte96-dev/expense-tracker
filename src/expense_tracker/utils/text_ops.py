import random
import string


def random_id(length=17):
    chars = string.digits + string.ascii_uppercase
    return "".join(random.choices(chars, k=length))


if __name__ == "__main__":
    print(random_id())
