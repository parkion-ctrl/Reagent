from django.db import models
from django.contrib.auth.models import User


class Inventory(models.Model):
    hazardous = models.TextField(blank=True, null=True)
    part = models.TextField()
    item_code = models.TextField()
    item_name = models.TextField()
    lot_no = models.TextField(blank=True, null=True)
    expiry_date = models.TextField(blank=True, null=True)
    spec = models.TextField(blank=True, null=True)
    unit = models.TextField(blank=True, null=True)
    reagent_type = models.TextField(blank=True, null=True)
    equipment = models.TextField(blank=True, null=True)
    vendor = models.TextField(blank=True, null=True)
    safety_stock = models.IntegerField(default=0)
    current_stock = models.IntegerField(default=0)
    required_qty = models.IntegerField(default=0)
    disposed_at = models.TextField(blank=True, null=True)
    disposal_reason = models.TextField(blank=True, null=True)
    disposal_type = models.TextField(blank=True, null=True)
    opened_at = models.TextField(blank=True, null=True)
    parallel_at = models.TextField(blank=True, null=True)
    base_item_name = models.TextField(blank=True, null=True)
    lot_status = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "inventory"


class TransactionHistory(models.Model):
    inventory = models.ForeignKey(Inventory, on_delete=models.DO_NOTHING, db_column="inventory_id")
    tx_type = models.TextField()
    qty = models.IntegerField()
    tx_date = models.TextField()
    note = models.TextField(blank=True, null=True)
    remaining_stock = models.IntegerField(default=0)
    item_code = models.TextField()
    item_name = models.TextField()
    lot_no = models.TextField(blank=True, null=True)
    part = models.TextField(blank=True, null=True)
    unit = models.TextField(blank=True, null=True)
    created_at = models.TextField()

    class Meta:
        managed = False
        db_table = "transaction_history"


class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="profile")
    employee_no = models.CharField(max_length=50, blank=True, default="")

    class Meta:
        db_table = "user_profile"
