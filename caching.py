import pickle
from pandas import DataFrame as DF, Series as S
import os
from inspect import isfunction
import hashlib


def Cache(dev=True, debug=False):
    stage = "dev" if dev else "prod"

    class cached:
        def __init__(self, fun_name):
            self.fun_name = fun_name
            self.reset_hash()

        def update_hash(self, value):
            if isinstance(value, DF):
                df = value
                hashed = str(df)
                hashed += "".join(df.columns.astype(str))
                hashed += "".join(df.iloc[:, 0].astype(str))
                hashed += "".join(df.iloc[:, -1].astype(str))

                value = hashed
                
            self.hash.update(str.encode(str(value)))

        def reset_hash(self):
            self.hash = hashlib.sha256()
            
        def get_hash(self):
            return self.hash.hexdigest()

        def serialize_arg(self, arg):
            if isinstance(arg, (list, tuple, set)):
                res = []
                for x in arg:
                    res.append(self.serialize_arg(x))
                return ",".join(res)

            if isfunction(arg):
                arg = arg.__name__
                if arg == "<lambda>":
                    raise ValueError("please pass named functions")

            self.update_hash(arg)
            if isinstance(arg, DF):
                return "df:shape:" + str(arg.shape)
            if isinstance(arg, S):
                return "series:len:" + str(len(arg))

            return str(arg)


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
                    + str(self.get_hash())
                )
                path = os.path.join("cache", stage, self.fun_name, f_hash)
                if os.path.exists(path):
                    if debug:
                        print(f"getting cached {path}")
                    with open(path, "rb") as file:
                        return pickle.load(file)

                result = fun(*args, **kwargs)
                os.makedirs(os.path.join("cache", stage, self.fun_name), exist_ok=True)
                with open(path, "wb") as file:
                    pickle.dump(result, file)

                return result

            return cached_f

    return cached
