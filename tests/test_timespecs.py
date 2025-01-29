import pytest

from datetime import timedelta, datetime, date

from harp.backend.timespec import RegularTimespec

from core.monitor import Chrono

def test_completeday_24steps_start0min():
    
    spec = RegularTimespec(timedelta(hours=0), 24)
    day = date(2012, 12, 12)
    
    times = spec.get_complete_day(day)
    
    for i in range(24):
        assert datetime(2012, 12, 12, i, 0) in times

def test_completeday_24steps_start25min():
    
    spec = RegularTimespec(timedelta(minutes=25), 24)
    day = date(2012, 12, 12)
    
    times = spec.get_complete_day(day)
    
    for i in range(24):
        assert datetime(2012, 12, 12, i, 25) in times


def test_intrasteps_intraday_24steps_start0min():
    
    spec = RegularTimespec(timedelta(hours=0), 24)
    time = datetime(2012, 12, 12, 17, 35, 0)
    
    times = spec.get_encompassing_timesteps(time)
    
    assert datetime(2012, 12, 12, 17, 0) in times
    assert datetime(2012, 12, 12, 18, 0) in times


def test_intrasteps_extraday_24steps_start0min():
    
    spec = RegularTimespec(timedelta(hours=0), 24)
    time = datetime(2012, 12, 12, 23, 35, 0)
    
    times = spec.get_encompassing_timesteps(time)
    
    assert datetime(2012, 12, 12, 23, 0) in times
    assert datetime(2012, 12, 13, 0, 0) in times


def test_intrasteps_intraday_24steps_start30min():
    
    spec = RegularTimespec(timedelta(minutes=30), 24)
    time = datetime(2012, 12, 12, 17, 35, 0)
    
    times = spec.get_encompassing_timesteps(time)
    
    assert datetime(2012, 12, 12, 17, 30) in times
    assert datetime(2012, 12, 12, 18, 30) in times


def test_intrasteps_extraday_24steps_start30min():
    
    spec = RegularTimespec(timedelta(minutes=30), 24)
    time = datetime(2012, 12, 12, 23, 35, 0)
    
    times = spec.get_encompassing_timesteps(time)
    
    assert datetime(2012, 12, 12, 23, 30) in times
    assert datetime(2012, 12, 13, 0, 30) in times


def test_exactstep_intraday_24steps():
    
    spec = RegularTimespec(timedelta(minutes=30), 24)
    time = datetime(2012, 12, 12, 23, 30, 0)
    
    times = spec.get_encompassing_timesteps(time)
    
    assert datetime(2012, 12, 12, 23, 30) in times
    assert len(times) == 1
    

def test_exactstep_intraday_8steps():
    
    spec = RegularTimespec(timedelta(hours=3), 8)
    time = datetime(2012, 12, 12, 9, 0, 0)
    
    times = spec.get_encompassing_timesteps(time)
    
    assert datetime(2012, 12, 12, 9, 0) in times
    assert len(times) == 1