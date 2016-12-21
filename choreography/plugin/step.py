# vim:fileencoding=utf-8

import abc
from choreography.cg_exception import CgException
from transitions import Machine
import asyncio
from asyncio import BaseEventLoop
from autologging import logged

class StepModelResponder(object):
    def run_step(self, nth_step):
        pass

    def run_idle(self, nth_step, duration):
        """
        Each step is expected to run :class:`StepModel`.*step_duration*. If you
        :class:`StepModel`.*ask* before *nth_step*'s completion, this function will be
        triggered with current *nth_step* and remaining *duration*.

        :note: When *nth_step*=0 . It marks the delay before the 1st step. In
        that case, *duration* is the *delay* set in StepModel.

        :param nth_step:
        :param duration:
        :return:
        """
        pass

    def run_done(self):
        pass


class StepModel(object):
    """
    A state machine that presents a monotonically increasing model.
    """
    states = ['created', 'start', 'step', 'idle', 'done']

    def __init__(self, responder: StepModelResponder, num_steps=-1,
                 step_duration=1, delay=0, loop: BaseEventLoop=None):
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
        # created -> start
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='start')
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

    def __str__(self, *args, **kwargs):
        return 'StepModel(responder={}, num_steps={}, step_duration={}, ' \
               'delay={}, loop: {})'.format(self.responder, self.num_steps,
                                            self.step_duration, self.delay,
                                            self.loop)

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

    def on_enter_start(self):
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


