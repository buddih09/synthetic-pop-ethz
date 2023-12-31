import numpy as np

# TODO: Maybe we can make the attribute writer a bit more generic? Maybe at some
# we want to define attributes for activities etc. (not sure if this is even
#possible now?)

class XmlWriter:
    def __init__(self, writer):
        self.writer = writer
        self.scope = []
        self.indent = 0

    def _write_line(self, content):
        self._write_indent()
        self._write(content + "\n")

    def _write_indent(self):
        self._write("  " * self.indent)

    def _write(self, content):
        self.writer.write(bytes(content, "utf-8"))

    def _require_scope(self, expected_scope):
        if expected_scope is None:
            if not len(self.scope) == 0:
                raise RuntimeError("Execpted initial scope")
            else:
                return
        else:
            if not type(expected_scope) == tuple and not type(expected_scope) == list:
                expected_scope = [expected_scope]

            if len(self.scope) == 0 or not self.scope[-1] in expected_scope:
                raise RuntimeError("Expected different scope")

    def _push_scope(self, scope):
        self.scope.append(scope)
        self.indent += 1

    def _pop_scope(self):
        del self.scope[-1]
        self.indent -= 1

    def yes_no(self, value):
        return "yes" if value else "no"

    def true_false(self, value):
        return "true" if value else "false"

    def time(self, time):
        time = int(time)
        hours = time // 3600
        minutes = (time % 3600) // 60
        seconds = (time % 60)
        return "%02d:%02d:%02d" % (hours, minutes, seconds)

    def location(self, x, y, facility_id = None):
        return (x, y,
                None if facility_id is None or (type(facility_id) == float and np.isnan(facility_id)) else facility_id)

class PopulationWriter(XmlWriter):
    POPULATION_SCOPE = 0
    # FINISHED_SCOPE = 1
    PERSON_SCOPE = 2
    PLAN_SCOPE = 3
    ATTRIBUTES_SCOPE = 4
    ACTIVITY_SCOPE = 5

    def __init__(self, writer):
        XmlWriter.__init__(self, writer)

    def start_population(self):
        self._require_scope(None)

        self._write_line('<?xml version="1.0" encoding="utf-8"?>')
        self._write_line('<!DOCTYPE population SYSTEM "http://www.matsim.org/files/dtd/population_v6.dtd">')
        self._write_line('<population desc="Switzerland Baseline">')

        self._push_scope(self.POPULATION_SCOPE)

    def end_population(self):
        self._require_scope(self.POPULATION_SCOPE)
        self._write_line('</population>')
        self._pop_scope()

    def start_person(self, person_id):
        self._require_scope(self.POPULATION_SCOPE)
        self._write_line('<person id="%s">' % person_id)
        self._push_scope(self.PERSON_SCOPE)

    def end_person(self):
        self._require_scope(self.PERSON_SCOPE)
        self._pop_scope()
        self._write_line('</person>')

    def start_attributes(self):
        self._require_scope([self.PERSON_SCOPE, self.ACTIVITY_SCOPE])
        self._write_line('<attributes>')
        self._push_scope(self.ATTRIBUTES_SCOPE)

    def end_attributes(self):
        self._require_scope(self.ATTRIBUTES_SCOPE)
        self._pop_scope()
        self._write_line('</attributes>')

    def add_attribute(self, name, type, value):
        self._require_scope(self.ATTRIBUTES_SCOPE)
        self._write_line('<attribute name="%s" class="%s">%s</attribute>' % (
            name, type, value
        ))

    def start_plan(self, selected):
        self._require_scope(self.PERSON_SCOPE)
        self._write_line('<plan selected="%s">' % self.yes_no(selected))
        self._push_scope(self.PLAN_SCOPE)

    def end_plan(self):
        self._require_scope(self.PLAN_SCOPE)
        self._pop_scope()
        self._write_line('</plan>')

    def _start_activity(self, type, location, start_time = None, end_time = None):
        self._write_indent()
        self._write('<activity ')
        self._write('type="%s" ' % type)
        self._write('x="%f" y="%f" ' % (location[0], location[1]))
        if location[2] is not None: self._write('facility="%s" ' % str(location[2]))
        if start_time is not None: self._write('start_time="%s" ' % self.time(start_time))
        if end_time is not None: self._write('end_time="%s" ' % self.time(end_time))

    def start_activity(self, type, location, start_time = None, end_time = None):
        self._require_scope(self.PLAN_SCOPE)
        self._start_activity(type, location, start_time, end_time)
        self._write('>\n')
        self._push_scope(self.ACTIVITY_SCOPE)

    def end_activity(self):
        self._require_scope(self.ACTIVITY_SCOPE)
        self._pop_scope()
        self._write_line('</activity>')

    def add_activity(self, type, location, start_time = None, end_time = None):
        self._require_scope(self.PLAN_SCOPE)

        self._start_activity(type, location, start_time, end_time)
        self._write('/>\n')

    def add_leg(self, mode, departure_time, travel_time):
        self._require_scope(self.PLAN_SCOPE)

        self._write_indent()
        self._write('<leg ')
        self._write('mode="%s" ' % mode)
        self._write('dep_time="%s" ' % self.time(departure_time))
        self._write('trav_time="%s" ' % self.time(travel_time))
        self._write('/>\n')

class HouseholdsWriter(XmlWriter):
    HOUSEHOLDS_SCOPE = 0
    FINISHED_SCOPE = 1
    HOUSEHOLD_SCOPE = 2
    ATTRIBUTES_SCOPE = 3

    def __init__(self, writer):
        XmlWriter.__init__(self, writer)

    def start_households(self):
        self._require_scope(None)
        self._write_line('<?xml version="1.0" encoding="utf-8"?>')
        self._write_line('<households xmlns="http://www.matsim.org/files/dtd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.matsim.org/files/dtd http://www.matsim.org/files/dtd/households_v1.0.xsd">')
        self._push_scope(self.HOUSEHOLDS_SCOPE)

    def end_households(self):
        self._require_scope(self.HOUSEHOLDS_SCOPE)
        self._write_line('</households>')
        self._pop_scope()

    def start_household(self, household_id):
        self._require_scope(self.HOUSEHOLDS_SCOPE)
        self._write_line('<household id="%d">' % household_id)
        self._push_scope(self.HOUSEHOLD_SCOPE)

    def end_household(self):
        self._require_scope(self.HOUSEHOLD_SCOPE)
        self._pop_scope()
        self._write_line('</household>')

    def start_attributes(self):
        self._require_scope(self.HOUSEHOLD_SCOPE)
        self._write_line('<attributes>')
        self._push_scope(self.ATTRIBUTES_SCOPE)

    def end_attributes(self):
        self._require_scope(self.ATTRIBUTES_SCOPE)
        self._pop_scope()
        self._write_line('</attributes>')

    def add_attribute(self, name, type, value):
        self._require_scope(self.ATTRIBUTES_SCOPE)
        self._write_line('<attribute name="%s" class="%s">%s</attribute>' % (
            name, type, value
        ))

    def add_members(self, person_ids):
        self._require_scope(self.HOUSEHOLD_SCOPE)
        self._write_line('<members>')
        self.indent += 1
        for person_id in person_ids: self._write_line('<personId refId="%s" />' % person_id)
        self.indent -= 1
        self._write_line('</members>')

    def add_income(self, income):
        self._require_scope(self.HOUSEHOLD_SCOPE)
        self._write_line('<income currency="CHF" period="month">%f</income>' % income)

class FacilitiesWriter(XmlWriter):
    FACILITIES_SCOPE = 0
    FINISHED_SCOPE = 1
    FACILITY_SCOPE = 2

    def __init__(self, writer):
        XmlWriter.__init__(self, writer)

    def start_facilities(self):
        self._require_scope(None)
        self._write_line('<?xml version="1.0" encoding="utf-8"?>')
        self._write_line('<!DOCTYPE facilities SYSTEM "http://www.matsim.org/files/dtd/facilities_v1.dtd">')
        self._write_line('<facilities name="Facilities from different sources">')
        self._push_scope(self.FACILITIES_SCOPE)

    def end_facilities(self):
        self._require_scope(self.FACILITIES_SCOPE)
        self._write_line('</facilities>')
        self._pop_scope()

    def start_facility(self, facility_id, x, y):
        self._require_scope(self.FACILITIES_SCOPE)
        self._write_line('<facility id="%s" x="%f" y="%f">' % (
            str(facility_id), x, y
        ))
        self._push_scope(self.FACILITY_SCOPE)

    def end_facility(self):
        self._require_scope(self.FACILITY_SCOPE)
        self._pop_scope()
        self._write_line('</facility>')

    def add_activity(self, purpose):
        self._require_scope(self.FACILITY_SCOPE)
        self._write_line('<activity type="%s" />' % purpose)
