import csv
from dataclasses import dataclass
from typing import Dict

from sqlalchemy.orm import joinedload

from rdr_service.model.study_nph import Order, StudyCategory
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'visit-period-backfill'
tool_desc = 'Backfill visit period'


@dataclass
class BackfillRecord:
    study_id: int
    new_visit_name: str
    order_id: str


class TimepointManager:
    def __init__(self, id_map: Dict[int, StudyCategory]):
        self.tp_map = id_map
        self.new_count = 0

        self.visit_name_map = {
            self.name_key(tp.parent.parent.id, tp.parent.name, tp.name): tp
            for tp in id_map.values()
            if tp.parent.type_label == 'visitPeriod'
        }
        print("count of existing visit periods: ", len(self.visit_name_map))

    def name_key(self, mo_id, visit_name, tp_name):
        return f'{mo_id} ||| {visit_name} ||| {tp_name}'

    def getTimepointName(self, tp_visit_name, visit_name, old_timepoint_id: StudyCategory, session):
        if tp_visit_name in self.visit_name_map:
            return self.visit_name_map[tp_visit_name]
        else:
            old_timepoint = self.getTimepointId(old_timepoint_id)
            module = old_timepoint.parent.parent

            new_visit = StudyCategory(
                type_label='visitPeriod',
                name=visit_name,
                parent=module
            )

            new_tp = StudyCategory(
                type_label=old_timepoint.type_label,
                name=old_timepoint.name,
                parent=new_visit
            )
            self.visit_name_map[tp_visit_name] = new_tp

            session.add(new_tp)
            session.flush()

            return new_tp

    def getTimepointId(self, id_):
        return self.tp_map[id_]


class VisitPeriodBackfill(ToolBase):
    def _timepoint_str(self, timepoint: StudyCategory):
        visit = timepoint.parent
        module = visit.parent

        return (
            f'{timepoint.id} ({module.name} ({module.id}) / {visit.name} ({visit.id}) /'
            f' {timepoint.name} ({timepoint.id}))s'
        )

    def _read_backfill_data(self, file_path):
        results = []
        with open(file_path) as file:
            reader = csv.DictReader(file)
            for record in reader:
                if record['orderID']:
                    results.append(
                        BackfillRecord(
                            study_id=int(record['studyID'][-1:]),
                            order_id=record['orderID'],
                            new_visit_name=record['visitID']
                        )
                    )
        return results

    def run(self):
        super().run()

        new_data = {}
        record_list = self._read_backfill_data(self.args.file)
        for record in record_list:
            if record.order_id not in new_data:
                new_data[record.order_id] = BackfillRecord(
                    study_id=record.study_id,
                    new_visit_name=record.new_visit_name,
                    order_id="none"
                )

        with self.get_session() as session:
            timepoint_query = session.query(StudyCategory).filter(
                StudyCategory.type_label == 'timepoint'
            ).options(
                joinedload(StudyCategory.parent).joinedload(StudyCategory.parent).joinedload(StudyCategory.parent)
            )
            timepoint_map = {
                timepoint.id: timepoint
                for timepoint in timepoint_query.all()
            }

            manager = TimepointManager(timepoint_map)

            order_list = session.query(Order).filter(Order.nph_order_id.in_(new_data.keys())).all()
            for order in order_list:
                current_timepoint = timepoint_map[order.category_id]
                update = new_data[order.nph_order_id]
                new_tp = manager.getTimepointName(
                    manager.name_key(current_timepoint.parent.parent.id, update.new_visit_name, current_timepoint.name),
                    update.new_visit_name,
                    order.category_id,
                    session
                )
                print(
                    f'{order.nph_order_id}'.ljust(15),
                    f'switching from'.ljust(18),
                    self._timepoint_str(current_timepoint).ljust(70),
                    f' to ',
                    self._timepoint_str(new_tp)
                )

                if order.category_id != new_tp.id:
                    order.category_id = new_tp.id


def add_additional_arguments(parser):
    parser.add_argument('--file', help="CSV to read new data from")


def run():
    return cli_run(tool_cmd, tool_desc, VisitPeriodBackfill, add_additional_arguments)
