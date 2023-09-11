from typing import Set

from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import with_session
from rdr_service.model.account_link import AccountLink


class AccountLinkDao:
    @classmethod
    @with_session
    def save_account_link(cls, account_link: AccountLink, session: Session):
        results = session.query(AccountLink).filter(
            AccountLink.participant_id == account_link.participant_id,
            AccountLink.related_id == account_link.related_id
        ).all()
        if not results:
            session.add(account_link)

    @classmethod
    @with_session
    def get_linked_ids(cls, participant_id: int, session: Session) -> Set[int]:
        """Returns the ids for any accounts linked to the given participant"""
        account_link_list = session.query(AccountLink).filter(
            AccountLink.participant_id == participant_id,
            AccountLink.get_active_filter()
        ).all()

        return {account_link.related_id for account_link in account_link_list}
