#
# Copyright 2020 Two Sigma Open Source, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import argparse
import uberjob
from uberjob.stores import JsonFileStore


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args()


def fahrenheit_to_celcius(f):
    return (f - 32) * 5 / 9


def build_plan(args):
    plan = uberjob.Plan()
    registry = uberjob.Registry()
    f = registry.source(plan, JsonFileStore(args.input_path))
    c = plan.call(fahrenheit_to_celcius, f)
    registry.add(c, JsonFileStore(args.output_path))
    return plan, registry


def main():
    args = parse_args()
    plan, registry = build_plan(args)
    uberjob.run(plan, registry=registry)


if __name__ == "__main__":
    main()
