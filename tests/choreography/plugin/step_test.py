# vim:fileencoding=utf-8


import abc
from transitions import Machine
import asyncio
from asyncio import BaseEventLoop
from unittest.mock import MagicMock
from autologging import logged

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
        self.__steps_remained = self.num_steps
        self.step_start_t = -1
        self.idle_start_t = -1

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
                                    condition=['is_step_finished'],
                                    after='step_to_idle')
        # step -> step
        self.machine.add_transition(trigger='ask', source='step',
                                    dest='step',
                                    unless=['is_step_finished'],
                                    condition=['has_steps'],
                                    after='step_to_step')
        # step -> done
        self.machine.add_transition(trigger='ask', source='step',
                                    dest='done',
                                    unless=['is_step_finished',
                                            'has_steps'])
        # =====================
        # idle -> idle
        self.machine.add_transition(trigger='ask', source='idle',
                                    dest='idle',
                                    unless=['is_idle_elapsed'],
                                    after='idle_to_idle')
        # idle -> done
        self.machine.add_transition(trigger='ask', source='idle',
                                    dest='done',
                                    condition=['is_idle_elapsed'],
                                    unless=['has_steps'])
        # idle -> step
        self.machine.add_transition(trigger='ask', source='idle',
                                    dest='step',
                                    condition=['is_idle_elapsed', 'has_steps'],
                                    after='idle_to_step')
        # =====================
        # done -> done
        self.machine.add_transition(trigger='ask', source='done', dest='done')

    def is_idle_elapsed(self):
        return self.idle_start_t + self.idle_duration <= self.loop.time()

    def is_step_finished(self):
        return self.step_start_t + self.step_duration <= self.loop.time()

    def step_to_idle(self):
        now = self.loop.time()
        self.step_start_t = -1
        self.idle_start_t = now
        duration = max(self.step_start_t + self.step_duration - now, 0)
        return self.responder.run_idle(self.current_step(), duration)

    def step_to_step(self):
        self.__steps_remained -= 1
        self.step_start_t = self.loop.time()
        self.responder.run_step(self.current_step())

    def idle_to_idle(self):
        duration = max(self.idle_start_t + self.idle_duration - self.loop.time(), 0)
        return self.responder.run_idle(self.current_step(), duration)

    def idle_to_step(self):
        self.__steps_remained -= 1
        self.step_start_t = self.loop.time()
        self.idle_start_t = -1
        self.responder.run_step(self.current_step())

    def on_enter_start(self):
        if self.delay > 0:
            self.idle_start_t = self.loop.time()
            self.to_idle()
            self.responder.run_idle(0, self.delay)
        elif self.has_steps():
            self.__steps_remained -= 1
            self.step_start_t = self.loop.time()
            self.to_step()
            self.responder.run_step(self.current_step())
        else:
            self.to_done()

    def on_enter_done(self):
        self.responder.run_done()

    def has_steps(self):
        return self.__steps_remained != 0

    def current_step(self):
        return self.num_steps - self.__steps_remained



if __name__ == '__main__':
    loop = MagicMock()
    loop.time.return_value = 1
    responder = MagicMock()
    m = StepModel(responder=responder, loop=loop)
    m.ask()
