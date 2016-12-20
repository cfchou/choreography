# vim:fileencoding=utf-8


import abc
from transitions import Machine
import asyncio
from asyncio import BaseEventLoop
from unittest.mock import MagicMock
from unittest.mock import patch
from autologging import logged
from nose import tools as nt
from unittest import skip

@logged
class StepModel(object):
    """
    A state machine that presents a monotonically increasing model.

    run_started,
    run_step(n)
    run_idle(n, t)
    run_done(n)
    """
    states = ['created', 'started', 'step', 'idle', 'done']

    def __init__(self, responder, num_steps=-1, step_duration=1, delay=0,
                 loop: BaseEventLoop=None):
        self.responder = responder
        # num_steps < 0 means infinite
        self.num_steps = int(num_steps)
        if step_duration < 0 or delay < 0:
            raise Exception('Invalid configs')
        self.step_duration = step_duration
        self.delay = delay
        self.loop = asyncio.get_event_loop() if loop is None else loop

        # internal
        self.steps_remained = self.num_steps
        self.step_until = -1
        self.idle_until = -1

        self.machine = Machine(model=self, states=StepModel.states,
                               initial='created')

        # =====================
        # created -> started
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='started')
        # =====================
        # step -> idle
        self.machine.add_transition(trigger='ask', source='step',
                                    dest='idle',
                                    unless=['is_step_finished'],
                                    after='step_to_idle')
        # step -> step
        self.machine.add_transition(trigger='ask', source='step',
                                    dest='step',
                                    conditions=['is_step_finished', 'has_steps'],
                                    after='step_to_step')
        # step -> done
        self.machine.add_transition(trigger='ask', source='step',
                                    dest='done',
                                    conditions=['is_step_finished'],
                                    unless=['has_steps'])
        # =====================
        # idle -> idle
        self.machine.add_transition(trigger='ask', source='idle',
                                    dest='idle',
                                    unless=['is_idle_elapsed'],
                                    after='idle_to_idle')
        # idle -> done
        self.machine.add_transition(trigger='ask', source='idle',
                                    dest='done',
                                    conditions=['is_idle_elapsed'],
                                    unless=['has_steps'])
        # idle -> step
        self.machine.add_transition(trigger='ask', source='idle',
                                    dest='step',
                                    conditions=['is_idle_elapsed', 'has_steps'],
                                    after='idle_to_step')
        # =====================
        # done -> done
        self.machine.add_transition(trigger='ask', source='done', dest='done')

    def is_idle_elapsed(self):
        return self.idle_until <= self.loop.time()

    def is_step_finished(self):
        return self.step_until <= self.loop.time()

    def step_to_idle(self):
        assert self.step_until != -1
        # now is earlier than step_until
        duration = max(self.step_until - self.loop.time(), 0)
        self.idle_until = self.step_until
        self.step_until = -1
        return self.responder.run_idle(self.current_step(), duration)

    def step_to_step(self):
        self.step_until = self.loop.time() + self.step_duration
        self.steps_remained -= 1
        self.responder.run_step(self.current_step())

    def idle_to_idle(self):
        assert self.idle_until != -1
        duration = max(self.idle_until - self.loop.time(), 0)
        return self.responder.run_idle(self.current_step(), duration)

    def idle_to_step(self):
        self.step_until = self.loop.time() + self.step_duration
        self.idle_until = -1
        self.steps_remained -= 1
        self.responder.run_step(self.current_step())

    def on_enter_started(self):
        if self.delay > 0:
            self.idle_until = self.loop.time() + self.delay
            self.to_idle()
            self.responder.run_idle(0, self.delay)
        elif self.has_steps():
            self.step_until = self.loop.time() + self.step_duration
            self.to_step()
            self.steps_remained -= 1
            self.responder.run_step(self.current_step())
        else:
            self.to_done()

    def on_enter_done(self):
        self.responder.run_done()

    def has_steps(self):
        return self.steps_remained != 0

    def current_step(self):
        return self.num_steps - self.steps_remained


def test_created():
    loop, responder = MagicMock(), MagicMock()
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
