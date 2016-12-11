# vim:fileencoding=utf-8

import abc
from choreography.cg_exception import CgException
from transitions import Machine
import asyncio
from asyncio import BaseEventLoop
from autologging import logged


class StepResponder(abc.ABC):
    """
    Implement this interface to react whenever a transition happens.
    """
    @abc.abstractmethod
    def run_step(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_offset(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_delay(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_idle(self):
        """
        :return:
        """

    @abc.abstractmethod
    def run_done(self):
        """
        :return:
        """


@logged
class StepModel(object):
    """
    A state machine that presents a monotonically increasing model.
    total = rate * num_steps + offset
    """
    states = ['created', 'delaying', 'offsetting', 'stepping', 'idling',
              'done']

    def __init__(self, num_steps=-1, step=1, offset=0, delay=0,
                 loop: BaseEventLoop=None):
        if step < 0 or delay < 0 or offset < 0:
            raise CgException('Invalid configs')

        # num_steps < 0 means infinite
        self.num_steps = int(num_steps)
        self.step = int(step)
        self.offset = int(offset)
        self.delay = delay
        self.loop = asyncio.get_event_loop() if loop is None else loop

        # internal
        self.__steps_remained = self.num_steps
        self.delay_start_t = 0
        self.step_start_t = 0

        self.machine = Machine(model=self, states=StepModel.states,
                               initial='created')

        # =====================
        # created -> delaying
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='delaying',
                                    conditions=['has_delay'],
                                    after='run_delay')
        # created -> offsetting
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='offsetting',
                                    conditions=['has_offset'],
                                    unless=['has_delay'],
                                    after='run_offset')
        # created -> stepping
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    unless=['has_offset', 'has_delay'],
                                    after='run_step')
        # created -> done
        self.machine.add_transition(trigger='ask', source='created',
                                    dest='done',
                                    unless=['has_offset', 'has_delay',
                                            'has_steps'],
                                    after='run_done')

        # =====================
        # delaying -> offsetting
        self.machine.add_transition(trigger='ask', source='delaying',
                                    dest='offsetting',
                                    conditions=['is_delay_elapsed',
                                                'has_offset'],
                                    after='run_offset')

        # delaying -> stepping
        self.machine.add_transition(trigger='ask', source='delaying',
                                    dest='stepping',
                                    conditions=['is_delay_elapsed',
                                                'has_steps'],
                                    unless=['has_offset'],
                                    after='run_step')
        # delaying -> done
        self.machine.add_transition(trigger='ask', source='delaying',
                                    dest='done',
                                    conditions=['is_delay_elapsed'],
                                    unless=['has_offset', 'has_steps'],
                                    after='run_done')

        # =====================
        # offsetting -> stepping
        self.machine.add_transition(trigger='ask', source='offsetting',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    after='run_step')
        # offsetting -> done
        self.machine.add_transition(trigger='ask', source='offsetting',
                                    dest='done',
                                    unless=['has_steps'],
                                    after='run_done')
        # =====================
        # stepping -> idling
        self.machine.add_transition(trigger='ask', source='stepping',
                                    dest='idling',
                                    conditions=['is_step_finished_early'],
                                    after='run_idle')
        # stepping -> stepping (explicitly transition to itself)
        self.machine.add_transition(trigger='ask', source='stepping',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    unless=['is_step_finished_early'],
                                    after='run_step')
        # stepping -> done
        self.machine.add_transition(trigger='ask', source='stepping',
                                    dest='done',
                                    unless=['is_step_finished_early',
                                            'has_steps'],
                                    after='run_done')
        # =====================
        # idling -> stepping
        self.machine.add_transition(trigger='ask', source='idling',
                                    dest='stepping',
                                    conditions=['has_steps'],
                                    unless=['is_step_finished_early'],
                                    after='run_step')
        # idling -> done
        self.machine.add_transition(trigger='ask', source='idling',
                                    dest='done',
                                    unless=['has_steps',
                                            'is_step_finished_early'],
                                    after='run_done')
        # =====================
        # done -> done (explicitly create a trivial transition)
        self.machine.add_transition(trigger='ask', source='done',
                                    dest='done',
                                    after='run_done')

    def on_enter_delaying(self):
        self.delay_start_t = self.loop.time()

    def on_enter_stepping(self):
        self.step_start_t = self.loop.time()
        self.__steps_remained -= 1

    def is_delay_elapsed(self):
        now = self.loop.time()
        return now >= self.delay_start_t + self.delay

    def is_step_finished_early(self):
        now = self.loop.time()
        return self.step_start_t + self.step > now

    def has_delay(self):
        return self.delay > 0

    def has_offset(self):
        return self.offset > 0

    def has_steps(self):
        return self.__steps_remained != 0

    def current_step(self):
        return self.num_steps - self.__steps_remained

    # ===== overrides ======
    def run_delay(self):
        pass

    def run_offset(self):
        pass

    def run_step(self):
        pass

    def run_idle(self):
        pass

    def run_done(self):
        pass



class StepRespModel(StepModel):
    """
    Execute implementation of StepResponder triggered by StepModel
    """
    def __init__(self, responder: StepResponder, num_steps=-1, step=1,
                 offset=0, delay=0, loop: BaseEventLoop=None):
        super().__init__(num_steps, step, offset, delay, loop)
        self.responder = responder

    def run_delay(self):
        self.responder.run_delay()

    def run_offset(self):
        self.responder.run_offset()

    def run_step(self):
        self.responder.run_step()

    def run_idle(self):
        self.responder.run_idle()

    def run_done(self):
        self.responder.run_done()

