#!/usr/bin/python3

from bisect import bisect_left
from datetime import date
import unittest
from palette_time import add_interval

def make_expected(exp_src):
    expected = []
    for stop_src in exp_src:
        stop = [ date.fromisoformat(stop_src[i]) for i in range(2) ]
        stop.append(stop_src[2])
        expected.append(stop)

        if stop_src[0] != stop_src[1]:
            expected.append([ date.fromisoformat(stop_src[1]) ])

    return expected


class Basics(unittest.TestCase):
    def test_pair(self):
        input_data = (
            (
                ( "1995-06-24", "1999-06-20", "orange" ),
                ( "1991-06-12", "1995-06-19", "orange" )
            ),
            (
                ( "1995-06-19", "1999-06-20", "orange" ),
                ( "1991-06-12", "1995-06-24", "orange" )
            ),
            (
                ( "1995-06-19", "1999-06-20", "orange" ),
                ( "1991-06-12", "1995-06-24", "red" )
            ),
            (
                ( "1991-06-19", "1999-06-20", "orange" ),
                ( "1995-06-24", "1995-06-24", "red" )
            ),
            (
                ( "1995-06-24", "1995-06-24", "red" ),
                ( "1991-06-19", "1999-06-20", "orange" )
            )
        )

        expected_data = (
            (
                ( "1991-06-12", "1995-06-19", "orange" ),
                ( "1995-06-24", "1999-06-20", "orange" )
            ),
            (
                ( "1991-06-12", "1999-06-20", "orange" ),
            ),
            (
                ( "1991-06-12", "1995-06-18", "red" ),
                ( "1995-06-19", "1999-06-20", "orange" )
            ),
            (
                ( "1991-06-19", "1995-06-23", "orange" ),
                ( "1995-06-24", "1995-06-24", "red" )
            ),
            (
                ( "1991-06-19", "1995-06-23", "orange" ),
                ( "1995-06-24", "1995-06-24", "red" )
            )
        )

        assert len(input_data) == len(expected_data)
        for i in range(len(input_data)):
            intervals = input_data[i]
            expected = make_expected(expected_data[i])

            person_obj = {}
            for from_str, until_str, color in intervals:
                add_interval(person_obj, date.fromisoformat(from_str), date.fromisoformat(until_str), color)

            self.assertEqual(len(person_obj.keys()), 1)
            self.assertEqual(person_obj['timed'], expected)

    def test_lookup(self):
        input_data = (
            (
                ( "1980-01-01", "1989-01-01", "red" ),
                ( "2013-10-30", "2017-10-26", "blue" )
            ),
            (
                ( "2012-01-01", "2020-01-01", "blue" ),
            )
        )

        test_data = (
            ( 4, "2018-03-09", 4 ),
            ( 2, "2018-06-12", 1 )
        )

        assert len(input_data) == len(test_data)
        for i in range(len(input_data)):
            intervals = input_data[i]
            test_val = test_data[i]

            person_obj = {}
            for from_str, until_str, color in intervals:
                add_interval(person_obj, date.fromisoformat(from_str), date.fromisoformat(until_str), color)

            timed = person_obj.get('timed')
            self.assertEqual(len(timed), test_val[0])
            i = bisect_left(timed, [ date.fromisoformat(test_val[1]) ])
            self.assertEqual(i, test_val[2])


if __name__ == "__main__":
    unittest.main()
