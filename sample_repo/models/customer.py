"""
SQLAlchemy ORM models for customer and account domain.
Includes PII fields — any function reading these must carry @pii_handler + @audit_required.
"""
from datetime import date, datetime
from sqlalchemy import (
    Column, String, Integer, BigInteger, Date, DateTime,
    Numeric, Boolean, Text, ForeignKey, Index
)
from sqlalchemy.orm import relationship
from config.database import Base


class Customer(Base):
    """
    Core customer master record.
    PII fields: ssn, date_of_birth, tax_id, email, phone_mobile, phone_home
    """
    __tablename__ = "customer_master"

    customer_id       = Column(BigInteger, primary_key=True, index=True)
    first_name        = Column(String(100), nullable=False)
    last_name         = Column(String(100), nullable=False)
    ssn               = Column(String(11), nullable=False)          # PII
    date_of_birth     = Column(Date, nullable=False)                # PII
    tax_id            = Column(String(20))                          # PII
    email             = Column(String(255))                         # PII
    phone_mobile      = Column(String(20))                          # PII
    phone_home        = Column(String(20))                          # PII
    address_line1     = Column(String(255))                         # PII
    address_city      = Column(String(100))
    address_state     = Column(String(2))
    address_zip       = Column(String(10))
    nationality       = Column(String(50))
    legacy_customer_id = Column(String(50))                        # DEPRECATED — use customer_id
    customer_segment  = Column(String(50))
    onboarding_date   = Column(Date)
    risk_rating       = Column(String(10))
    is_active         = Column(Boolean, default=True)
    kyc_status        = Column(String(20), default="PENDING")
    kyc_verified_at   = Column(DateTime)
    created_at        = Column(DateTime, default=datetime.utcnow)
    updated_at        = Column(DateTime, onupdate=datetime.utcnow)

    accounts     = relationship("Account",      back_populates="customer")
    risk_scores  = relationship("RiskScore",    back_populates="customer")

    __table_args__ = (
        Index("ix_customer_ssn", "ssn"),
        Index("ix_customer_segment", "customer_segment"),
    )


class Account(Base):
    """
    Customer account (checking, savings, loan, credit).
    """
    __tablename__ = "accounts"

    account_id       = Column(BigInteger, primary_key=True, index=True)
    customer_id      = Column(BigInteger, ForeignKey("customer_master.customer_id"), nullable=False)
    account_type     = Column(String(30))   # CHECKING | SAVINGS | LOAN | CREDIT_CARD
    account_number   = Column(String(30), unique=True)
    routing_number   = Column(String(9))
    currency         = Column(String(3), default="USD")
    current_balance  = Column(Numeric(18, 2), default=0.00)
    available_balance = Column(Numeric(18, 2), default=0.00)
    credit_limit     = Column(Numeric(18, 2))
    interest_rate    = Column(Numeric(8, 6))
    open_date        = Column(Date)
    close_date       = Column(Date)
    status           = Column(String(20), default="ACTIVE")
    product_code     = Column(String(20))
    branch_id        = Column(Integer)

    customer     = relationship("Customer",     back_populates="accounts")
    transactions = relationship("Transaction",  back_populates="account")


class Transaction(Base):
    """
    Customer transaction record (deposits, withdrawals, transfers).
    """
    __tablename__ = "transactions"

    transaction_id   = Column(BigInteger, primary_key=True)
    account_id       = Column(BigInteger, ForeignKey("accounts.account_id"), nullable=False)
    transaction_type = Column(String(30))   # DEBIT | CREDIT | TRANSFER | FEE
    amount           = Column(Numeric(18, 2), nullable=False)
    currency         = Column(String(3), default="USD")
    fx_rate          = Column(Numeric(12, 6), default=1.0)
    amount_usd       = Column(Numeric(18, 2))
    description      = Column(String(500))
    merchant_id      = Column(String(50))
    merchant_name    = Column(String(200))
    merchant_category = Column(String(50))
    channel          = Column(String(30))   # BRANCH | ATM | ONLINE | MOBILE
    transaction_date  = Column(Date, nullable=False)
    posted_date       = Column(Date)
    value_date        = Column(Date)
    reference_number = Column(String(100))
    status           = Column(String(20), default="POSTED")
    is_suspicious    = Column(Boolean, default=False)
    aml_score        = Column(Numeric(5, 4))
    # DEPRECATED — old raw amount before FX normalisation
    old_amount       = Column(Numeric(18, 4))   # DEPRECATED: use amount_usd

    account = relationship("Account", back_populates="transactions")

    __table_args__ = (
        Index("ix_txn_account_date",  "account_id", "transaction_date"),
        Index("ix_txn_merchant",      "merchant_id"),
        Index("ix_txn_suspicious",    "is_suspicious"),
    )
