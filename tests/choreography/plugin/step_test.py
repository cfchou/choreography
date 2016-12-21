# vim:fileencoding=utf-8


import abc
from transitions import Machine
import asyncio
from choreography.plugin.step import StepModel, StepModelResponder
from asyncio import BaseEventLoop
from unittest.mock import MagicMock
from unittest.mock import patch
from autologging import logged
from nose import tools as nt
from unittest import skip
import attr
from attr import validators




def test_created():
    loop = MagicMock(spec=BaseEventLoop)
    responder = MagicMock(spec=StepModelResponder)
    m = StepModel(responder=responder, loop=loop, num_steps=0)
    nt.assert_equal(m.state, 'created')


def test_done():
    loop, responder = MagicMock(), MagicMock()
    m = StepModel(responder=responder, loop=loop, num_steps=0)
    m.ask()
    responder.run_done.assert_called_once_with()
    nt.assert_equal(m.state, 'done')


def test_step_done():
    loop, responder = MagicMock(), MagicMock()
    m = StepModel(responder=responder, loop=loop, num_steps=1)
    loop.time.return_value = 1
    m.ask()
    responder.run_step.assert_called_once_with(1)
    nt.assert_equal(m.state, 'step')

    responder.run_step.reset_mock()
    loop.time.return_value = 1 + m.step_duration
    m.ask()
    responder.run_step.assert_not_called()
    responder.run_done.assert_called_once_with()
    nt.assert_equal(m.state, 'done')


def test_delay_step():
    loop, responder = MagicMock(), MagicMock()
    m = StepModel(responder=responder, loop=loop, delay=10)

    loop.time.return_value = 1
    m.ask()
    responder.run_idle.assert_called_with(0, 10)
    nt.assert_equal(m.state, 'idle')

    loop.time.return_value = 2
    m.ask()
    responder.run_idle.assert_called_with(0, 9)
    nt.assert_equal(m.state, 'idle')

    responder.run_idle.reset_mock()
    loop.time.return_value = 11
    m.ask()
    responder.run_idle.assert_not_called()
    responder.run_step.assert_called_with(1)
    nt.assert_equal(m.state, 'step')


def test_delay_done():
    loop, responder = MagicMock(), MagicMock()
    m = StepModel(responder=responder, loop=loop, delay=10, num_steps=0)

    loop.time.return_value = 1
    m.ask()
    responder.run_idle.assert_called_with(0, 10)
    nt.assert_equal(m.state, 'idle')

    responder.run_idle.reset_mock()
    loop.time.return_value = 11
    m.ask()
    responder.run_idle.assert_not_called()
    responder.run_done.assert_called_once_with()
    nt.assert_equal(m.state, 'done')


def test_steps():
    loop, responder = MagicMock(), MagicMock()
    m = StepModel(responder=responder, loop=loop, num_steps=3, step_duration=3)

    loop.time.return_value = 1
    m.ask()
    responder.run_step.assert_called_with(1)
    nt.assert_equal(m.state, 'step')

    # step#2 can begin at 4, but we come late
    loop.time.return_value = 5
    m.ask()
    responder.run_step.assert_called_with(2)
    nt.assert_equal(m.state, 'step')

    # step#3 can begin at 8, so we'll idle
    loop.time.return_value = 7
    m.ask()
    responder.run_idle.assert_called_with(2, 1)
    nt.assert_equal(m.state, 'idle')

    # step#3 begin at 8, we come on time
    loop.time.return_value = 8
    m.ask()
    responder.run_step.assert_called_with(3)
    nt.assert_equal(m.state, 'step')

    # step#3 lasts until 11, and we're done
    loop.time.return_value = 11
    m.ask()
    responder.run_done.assert_called_with()
    nt.assert_equal(m.state, 'done')


if __name__ == '__main__':
    loop = MagicMock()
    loop.time.return_value = 1
    responder = MagicMock()
    m = StepModel(responder=responder, loop=loop)
    m.ask()
