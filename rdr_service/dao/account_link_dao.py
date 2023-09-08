from typing import Set

from sqlalchemy import or_
from sqlalchemy.orm import Session

from rdr_service.clock import CLOCK
from rdr_service.dao.base_dao import with_session
from rdr_service.model.account_link import AccountLink


class AccountLinkDao:
    @classmethod
    @with_session
    def save_account_link(cls, account_link: AccountLink, session: Session):
        # todo: prevent saving duplicated account links
        session.add(account_link)

    @classmethod
    @with_session
    def get_linked_ids(cls, participant_id: int, session: Session) -> Set[int]:
        """Returns the ids for any accounts linked to the given participant"""
        now_datetime = CLOCK.now()
        account_link_list = session.query(AccountLink).filter(
            AccountLink.participant_id == participant_id,
            or_(AccountLink.start.is_(None), AccountLink.start < now_datetime),
            or_(AccountLink.end.is_(None), AccountLink.end > now_datetime)
        ).all()

        return {account_link.related_id for account_link in account_link_list}
