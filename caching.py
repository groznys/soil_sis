import pickle
from pandas import DataFrame as DF, Series as S
import os
from inspect import isfunction


class cached:
    def __init__(self, fun_name):
        self.fun_name = fun_name
        self.reset_hash()

    def update_hash(self, value):
        self.hash = hash(str(self.hash) + str(value))

    def reset_hash(self):
        self.hash = 0

    def serialize_arg(self, arg):
        if isinstance(arg, (list, tuple, set)):
            res = []
            for x in arg:
                res.append(self.serialize_arg(x))
            return "-".join(res)

        if isfunction(arg):
            arg = arg.__name__
            if arg == "<lambda>":
                raise ValueError("please pass named functions")

        self.update_hash(arg)
        if isinstance(arg, DF):
            return "df:shape:" + str(arg.shape)
        if isinstance(arg, S):
            return "series:len:" + str(len(arg))

        try:
            return str(arg)
        except:
            return str(hash(arg))

    def serialize_args(self, args):
        return "-".join(self.serialize_arg(arg) for arg in args)

    def serialize_kwargs(self, kwargs):
        return "-".join(k + ":" + self.serialize_arg(v) for k, v in kwargs.items())

    def __call__(self, fun):
        self.reset_hash()

        def cached_f(*args, **kwargs):
            self.reset_hash()
            f_hash = (
                self.serialize_args(args)
                + "#"
                + self.serialize_kwargs(kwargs)
                + "#"
                + str(self.hash)
            )
            path = os.path.join("cache", self.fun_name, f_hash)
            if os.path.exists(path):
                with open(path, "rb") as file:
                    return pickle.load(file)

            result = fun(*args, **kwargs)
            os.makedirs(os.path.join("cache", self.fun_name), exist_ok=True)
            with open(path, "wb") as file:
                pickle.dump(result, file)

            return result

        return cached_f
