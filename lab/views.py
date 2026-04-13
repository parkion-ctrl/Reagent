import io
import json
from urllib.parse import quote_plus

import pandas as pd
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import Group, Permission, User
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.contrib.auth.decorators import user_passes_test
from django.contrib.auth.hashers import make_password
from django.db.models import Q
from django.views.decorators.csrf import csrf_exempt

from app.core.db import get_connection, get_current_schema
from app.services.history_service import get_history_items
from app.services.inbound_service import (
    create_bulk_inbound_transactions,
    get_inbound_page_data,
    preview_bulk_inbound_items,
    preview_manual_inbound_items,
)
from app.services.inventory_service import get_inventory_filter_options, get_inventory_items
from app.services.master_service import (
    confirm_bulk_master_items,
    create_master_item,
    delete_master_item,
    get_master_item_by_id,
    get_master_items,
    preview_bulk_master_items_v3,
    update_master_item,
)
from app.services.outbound_service import (
    create_bulk_outbound_transactions,
    get_outbound_page_data,
    preview_bulk_outbound_items,
    preview_manual_outbound_items,
)
from app.services.reagent_history_service import (
    dispose_reagent,
    get_reagent_history_filter_options,
    get_reagent_history_items,
    get_old_new_lot_items,
    save_old_new_lot_selection,
    update_opened_at,
    update_parallel_at,
)
from .models import Inventory, TransactionHistory, UserProfile
from app.utils.constants import PART_MAP, get_part_map


def can_access_admin_area(user):
    if not user.is_authenticated:
        return False
    if user.is_superuser:
        return True
    return user.groups.filter(name__in=["관리자", "개발자"]).exists()


def _is_dlab(request):
    """현재 활성 스키마가 진단검사의학과(dlab)인지 여부."""
    return get_current_schema() == "dlab"


def _get_part(request):
    """파트 파라미터를 반환한다."""
    return request.GET.get("part", "")


@csrf_exempt
def login_page(request):
    error = ""
    next_url = request.POST.get("next") if request.method == "POST" else request.GET.get("next", "")
    if request.method == "POST":
        username = request.POST.get("username", "").strip()
        password = request.POST.get("password", "")
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            target_url = next_url or "/inventory/"
            if target_url in {"", "/"} or target_url.startswith("/login"):
                target_url = "/inventory/"
            return redirect(target_url)
        try:
            existing = User.objects.get(username=username)
            if not existing.is_active:
                error = "비활성화된 계정입니다. 관리자에게 문의하세요."
            else:
                error = "아이디 또는 비밀번호가 올바르지 않습니다."
        except User.DoesNotExist:
            error = "아이디 또는 비밀번호가 올바르지 않습니다."

    return render(request, "login.html", {"error": error, "next": next_url})


@login_required
def logout_view(request):
    logout(request)
    return redirect("login")


@login_required
def set_dept_view(request):
    """슈퍼유저 전용: 세션의 활성 부서를 전환합니다."""
    if not request.user.is_superuser:
        return redirect(request.GET.get("next", "/inventory/"))
    dept = request.GET.get("dept", "")
    from app.utils.constants import DEPT_SCHEMA_MAP
    if dept in DEPT_SCHEMA_MAP:
        request.session["superuser_active_dept"] = dept
    else:
        request.session.pop("superuser_active_dept", None)
    next_url = request.GET.get("next", "/inventory/")
    return redirect(next_url)


@login_required
@user_passes_test(can_access_admin_area)
def admin_panel(request):
    from datetime import date, timedelta

    one_month_ago = (date.today() - timedelta(days=30)).isoformat()

    # 현재 부서 파악
    if request.user.is_superuser:
        active_dept = request.session.get("superuser_active_dept", "진단검사의학과")
    else:
        profile = UserProfile.objects.filter(user=request.user).first()
        active_dept = profile.department if profile else ""

    user_count = User.objects.filter(profile__department=active_dept).count() if active_dept else User.objects.count()

    # 시약/이력은 get_connection()으로 현재 스키마에서 직접 집계
    inventory_count = 0
    history_count = 0
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM inventory WHERE disposed_at IS NULL")
        inventory_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM transaction_history WHERE tx_date >= %s", (one_month_ago,))
        history_count = cur.fetchone()[0]
        conn.close()
    except Exception:
        pass

    return render(
        request,
        "admin_panel.html",
        {
            "active_menu": "admin_panel",
            "stats": {
                "users": user_count,
                "groups": Group.objects.count(),
                "inventory": inventory_count,
                "history": history_count,
            },
        },
    )


@login_required
@user_passes_test(can_access_admin_area)
def admin_users(request):
    from lab.models import DEPARTMENT_CHOICES
    is_superadmin = request.user.is_superuser or request.user.groups.filter(name="개발자").exists()

    my_profile = UserProfile.objects.filter(user=request.user).first()
    my_department = my_profile.department if my_profile else ""

    # 개발자/superuser만 부서 전환 가능, 나머지는 자기 부서 고정
    if is_superadmin:
        selected_department = request.GET.get("department", my_department).strip()
    else:
        selected_department = my_department

    q = request.GET.get("q", "").strip()
    selected_part = request.GET.get("part", "").strip()
    users = User.objects.select_related("profile").all().order_by("username")

    if selected_department:
        users = users.filter(profile__department=selected_department)
    if q:
        users = users.filter(
            Q(username__icontains=q)
            | Q(first_name__icontains=q)
            | Q(last_name__icontains=q)
            | Q(profile__employee_no__icontains=q)
        ).distinct().order_by("username")
    if selected_part:
        users = users.filter(profile__part=selected_part)

    profile_map = {
        profile.user_id: profile
        for profile in UserProfile.objects.filter(user_id__in=[user.id for user in users])
    }
    users_data = []
    for user in users:
        group_names = [group.name for group in user.groups.all()]
        if group_names:
            role_labels = group_names
        elif user.is_superuser or user.is_staff:
            role_labels = ["시스템 관리자"]
        else:
            role_labels = ["그룹 없음"]
        profile = profile_map.get(user.id)
        part_code = profile.part if profile else ""
        users_data.append(
            {
                "user": user,
                "employee_no": profile.employee_no if profile else "",
                "part": f"{part_code} ({get_part_map(get_current_schema()).get(part_code, '')})" if part_code else part_code,
                "department": profile.department if profile else "",
                "group_names": group_names,
                "role_labels": role_labels,
            }
        )
    return render(
        request,
        "admin_users.html",
        {
            "active_menu": "admin_panel",
            "users": users_data,
            "q": q,
            "selected_part": selected_part,
            "selected_department": selected_department,
            "part_map": get_part_map(get_current_schema()),
            "department_choices": [c[0] for c in DEPARTMENT_CHOICES],
            "is_superadmin": is_superadmin,
        },
    )


@login_required
@login_required
@user_passes_test(can_access_admin_area)
def admin_user_form(request, user_id=None):
    user_obj = User.objects.filter(id=user_id).first() if user_id else None
    errors = []
    role_group_names = ["개발자", "관리자", "일반", "외부 업체"]
    role_groups = []
    for group_name in role_group_names:
        group_obj, _ = Group.objects.get_or_create(name=group_name)
        role_groups.append(group_obj)

    selected_group_id = 0
    if user_obj:
        current_groups = list(user_obj.groups.order_by("name"))
        if current_groups:
            selected_group_id = current_groups[0].id

    if request.method == "POST":
        username = request.POST.get("username", "").strip()
        employee_no = request.POST.get("employee_no", "").strip()
        part = request.POST.get("part", "").strip()
        department = request.POST.get("department", "").strip()
        first_name = request.POST.get("first_name", "").strip()
        last_name = request.POST.get("last_name", "").strip()
        password = request.POST.get("password", "")
        is_active = request.POST.get("is_active", "on") == "on"
        selected_group_id = int(request.POST.get("group_id") or 0)

        if not username:
            errors.append("아이디를 입력해 주세요.")
        if user_obj is None and not password:
            errors.append("새 사용자에게는 비밀번호가 필요합니다.")
        is_developer = selected_group_id and Group.objects.filter(id=selected_group_id, name="개발자").exists()
        if not is_developer:
            if not selected_group_id:
                errors.append("권한 그룹을 선택해 주세요.")
            if not department:
                errors.append("부서를 선택해 주세요.")
            if not part:
                errors.append("파트를 선택해 주세요.")

        if not errors:
            target = user_obj or User()
            target.username = username
            target.first_name = first_name
            target.last_name = last_name
            target.is_active = is_active
            if password:
                target.password = make_password(password)
            selected_group = Group.objects.filter(id=selected_group_id).first()
            if selected_group and selected_group.name == "개발자":
                target.is_superuser = True
                target.is_staff = True
            elif selected_group and selected_group.name == "관리자":
                target.is_superuser = False
                target.is_staff = True
            else:
                target.is_superuser = False
                target.is_staff = False
            target.save()
            if selected_group:
                target.groups.set([selected_group])
            else:
                target.groups.clear()
            profile, _ = UserProfile.objects.get_or_create(user=target)
            profile.employee_no = employee_no
            profile.part = part
            profile.department = department
            profile.save()
            return redirect("/admin-users/?message=저장되었습니다.")

    profile_obj = UserProfile.objects.filter(user_id=user_obj.id).first() if user_obj else None
    context = {
        "active_menu": "admin_panel",
        "user_obj": user_obj,
        "profile_employee_no": profile_obj.employee_no if profile_obj else "",
        "profile_part": profile_obj.part if profile_obj else "",
        "profile_department": profile_obj.department if profile_obj else "",
        "groups": role_groups,
        "selected_group_id": selected_group_id,
        "part_map": get_part_map(get_current_schema()),
        "department_choices": ["진단검사의학과", "병리과", "핵의학과", "유해물질"],
        "errors": errors,
    }
    return render(request, "admin_user_form.html", context)


@login_required
@user_passes_test(can_access_admin_area)
@csrf_exempt
def admin_user_delete(request, user_id):
    if request.method != "POST":
        return redirect("admin_users")
    if request.user.id == user_id:
        return redirect("/admin-users/?error=자기 자신은 삭제할 수 없습니다.")
    User.objects.filter(id=user_id).delete()
    return redirect("/admin-users/?message=삭제되었습니다.")


@login_required
@user_passes_test(can_access_admin_area)
def admin_groups(request):
    q = request.GET.get("q", "").strip()
    groups = Group.objects.all().order_by("name")
    if q:
        groups = groups.filter(name__icontains=q)
    return render(
        request,
        "admin_groups.html",
        {
            "active_menu": "admin_panel",
            "groups": groups,
            "q": q,
        },
    )


@login_required
@user_passes_test(can_access_admin_area)
def admin_group_form(request, group_id=None):
    group_obj = Group.objects.filter(id=group_id).first() if group_id else None
    permissions = Permission.objects.select_related("content_type").order_by("content_type__app_label", "content_type__model", "codename")
    errors = []
    if request.method == "POST":
        name = request.POST.get("name", "").strip()
        permission_ids = [int(v) for v in request.POST.getlist("permissions") if str(v).strip()]
        selected_permission_ids = permission_ids
        if not name:
            errors.append("그룹명을 입력해 주세요.")
        if not errors:
            target = group_obj or Group()
            target.name = name
            target.save()
            target.permissions.set(Permission.objects.filter(id__in=permission_ids))
            return redirect("/admin-groups/?message=저장되었습니다.")
    return render(
        request,
        "admin_group_form.html",
        {
            "active_menu": "admin_panel",
            "group_obj": group_obj,
            "permissions": permissions,
            "selected_permission_ids": selected_permission_ids,
            "errors": errors,
        },
    )


@login_required
@user_passes_test(can_access_admin_area)
@csrf_exempt
def admin_group_delete(request, group_id):
    if request.method != "POST":
        return redirect("admin_groups")
    Group.objects.filter(id=group_id).delete()
    return redirect("/admin-groups/?message=삭제되었습니다.")


@login_required
@user_passes_test(can_access_admin_area)
def admin_parts(request):
    from lab.models import Part
    from app.core.db import ALL_SCHEMAS
    import psycopg
    from app.core.db import PG_HOST, PG_PORT, PG_DBNAME, PG_USER, PG_PASSWORD

    message = request.GET.get("message", "")
    current_schema = get_current_schema()
    # dlab은 schema_name=None, 나머지는 해당 schema_name으로 필터
    if current_schema == "dlab" or current_schema is None:
        qs = Part.objects.filter(schema_name__isnull=True)
    else:
        qs = Part.objects.filter(schema_name=current_schema)
    parts = list(qs)

    # 현재 부서 스키마 기준 파트별 사용 건수 집계
    usage = {}
    try:
        schemas = ALL_SCHEMAS if (current_schema == "dlab" or current_schema is None) else ([current_schema] if current_schema else [])
        conn = psycopg.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
                               user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()
        for schema in schemas:
            cur.execute(f"SELECT part, COUNT(*) FROM {schema}.inventory WHERE disposed_at IS NULL GROUP BY part")
            for row in cur.fetchall():
                code = (row[0] or "").strip()
                usage[code] = usage.get(code, 0) + int(row[1])
        conn.close()
    except Exception:
        pass

    parts_data = [{"obj": p, "usage": usage.get(p.code, 0)} for p in parts]

    return render(request, "admin_parts.html", {
        "active_menu": "admin_panel",
        "parts_data": parts_data,
        "message": message,
    })


@login_required
@user_passes_test(can_access_admin_area)
def admin_part_form(request, part_id=None):
    from lab.models import Part
    current_schema = get_current_schema()
    # dlab은 schema_name=None, 나머지는 해당 schema_name
    save_schema = None if (current_schema == "dlab" or current_schema is None) else current_schema

    part_obj = Part.objects.filter(id=part_id).first() if part_id else None
    errors = []

    if request.method == "POST":
        code = request.POST.get("code", "").strip().upper()
        name = request.POST.get("name", "").strip()

        if not code:
            errors.append("파트 코드는 필수입니다.")
        if not name:
            errors.append("파트명은 필수입니다.")
        if code and Part.objects.filter(code=code, schema_name=save_schema).exclude(id=part_id).exists():
            errors.append(f"파트 코드 '{code}'는 이미 존재합니다.")

        if not errors:
            if part_obj:
                part_obj.code = code
                part_obj.name = name
                part_obj.save()
            else:
                Part.objects.create(code=code, name=name, schema_name=save_schema)
            return redirect("/admin-parts/?message=저장되었습니다.")

    return render(request, "admin_part_form.html", {
        "active_menu": "admin_panel",
        "part_obj": part_obj,
        "errors": errors,
    })


@login_required
@user_passes_test(can_access_admin_area)
@csrf_exempt
def admin_part_delete(request, part_id):
    if request.method != "POST":
        return redirect("admin_parts")
    from lab.models import Part
    Part.objects.filter(id=part_id).delete()
    return redirect("/admin-parts/?message=삭제되었습니다.")


@login_required
def root_redirect(request):
    return redirect("login")


def get_master_base_context():
    return {
        "active_menu": "master",
        "items": get_master_items(),
        "part_map": get_part_map(get_current_schema()),
        "part": "",
        "q": "",
        "sort": "",
        "order": "",
        "show_form": "",
        "message": "",
        "error": "",
        "reagent_type": "",
        "edit_item": None,
        "equipment": "",
        "vendor": "",
        "hazardous": "",
        "filter_options": get_inventory_filter_options(),
    }


@login_required
def inventory_page(request):
    part = _get_part(request)
    q = request.GET.get("q", "")
    reagent_type = request.GET.get("reagent_type", "")
    equipment = request.GET.get("equipment", "")
    vendor = request.GET.get("vendor", "")
    hazardous = request.GET.get("hazardous", "")
    expiry_filter = request.GET.get("expiry_filter", "")
    sort = request.GET.get("sort", "")
    order = request.GET.get("order", "")

    context = {
        "active_menu": "inventory",
        "items": get_inventory_items(
            part=part,
            q=q,
            reagent_type=reagent_type,
            equipment=equipment,
            vendor=vendor,
            hazardous=hazardous,
            expiry_filter=expiry_filter,
            sort=sort,
            order=order,
        ),
        "part_map": get_part_map(get_current_schema()),
        "part": part,
        "q": q,
        "reagent_type": reagent_type,
        "equipment": equipment,
        "vendor": vendor,
        "hazardous": hazardous,
        "expiry_filter": expiry_filter,
        "sort": sort,
        "order": order,
        "filter_options": get_inventory_filter_options(),
    }
    return render(request, "inventory.html", context)


@login_required
def master_page(request):
    part = _get_part(request)
    q = request.GET.get("q", "")
    sort = request.GET.get("sort", "")
    order = request.GET.get("order", "")
    show_form = request.GET.get("show_form", "")
    edit_id = request.GET.get("edit_id", "")
    message = request.GET.get("message", "")
    error = request.GET.get("error", "")
    reagent_type = request.GET.get("reagent_type", "")
    equipment = request.GET.get("equipment", "")
    vendor = request.GET.get("vendor", "")
    hazardous = request.GET.get("hazardous", "")

    context = {
        "active_menu": "master",
        "items": get_master_items(
            part=part,
            q=q,
            reagent_type=reagent_type,
            equipment=equipment,
            vendor=vendor,
            hazardous=hazardous,
            sort=sort,
            order=order,
        ),
        "part_map": get_part_map(get_current_schema()),
        "part": part,
        "q": q,
        "sort": sort,
        "order": order,
        "show_form": show_form,
        "message": message,
        "error": error,
        "reagent_type": reagent_type,
        "edit_item": get_master_item_by_id(int(edit_id)) if edit_id else None,
        "equipment": equipment,
        "vendor": vendor,
        "hazardous": hazardous,
        "filter_options": get_inventory_filter_options(),
    }
    return render(request, "master.html", context)


@csrf_exempt
@login_required
def master_create(request):
    if request.method != "POST":
        return redirect("master")

    ok, msg = create_master_item(
        hazardous=request.POST.get("hazardous", ""),
        part=request.POST.get("part", ""),
        item_code=request.POST.get("item_code", ""),
        item_name=request.POST.get("item_name", ""),
        lot_no=request.POST.get("lot_no", ""),
        expiry_date=request.POST.get("expiry_date", ""),
        spec=request.POST.get("spec", ""),
        unit=request.POST.get("unit", ""),
        reagent_type=request.POST.get("reagent_type", ""),
        equipment=request.POST.get("equipment", ""),
        vendor=request.POST.get("vendor", ""),
        safety_stock=int(request.POST.get("safety_stock", 0) or 0),
    )
    if ok:
        return redirect("/master/?message=항목이 등록되었습니다.")
    return redirect(f"/master/?show_form=1&error={quote_plus(msg)}")


@csrf_exempt
@login_required
def master_edit_submit(request, item_id: int):
    if request.method != "POST":
        return redirect("master")

    update_master_item(
        item_id=item_id,
        hazardous=request.POST.get("hazardous", ""),
        part=request.POST.get("part", ""),
        item_code=request.POST.get("item_code", ""),
        item_name=request.POST.get("item_name", ""),
        lot_no=request.POST.get("lot_no", ""),
        expiry_date=request.POST.get("expiry_date", ""),
        spec=request.POST.get("spec", ""),
        unit=request.POST.get("unit", ""),
        reagent_type=request.POST.get("reagent_type", ""),
        equipment=request.POST.get("equipment", ""),
        vendor=request.POST.get("vendor", ""),
        safety_stock=int(request.POST.get("safety_stock", 0) or 0),
    )
    return redirect("/master/?message=항목이 수정되었습니다.")


@csrf_exempt
@login_required
def master_delete(request, item_id: int):
    if request.method != "POST":
        return redirect("master")
    delete_master_item(item_id)
    return redirect("/master/?message=항목이 삭제되었습니다.")


@csrf_exempt
@login_required
def master_dispose(request, item_id: int):
    if request.method != "POST":
        return redirect("master")
    dispose_reagent(item_id=item_id, reason="자동 폐기", disposal_type="MANUAL")
    return redirect("/master/?message=시약이 폐기되었습니다.")


@csrf_exempt
@login_required
def master_bulk_delete(request):
    if request.method != "POST":
        return redirect("master")
    item_ids = [int(item_id) for item_id in request.POST.getlist("item_ids") if str(item_id).strip()]
    for item_id in item_ids:
        delete_master_item(item_id)
    return redirect(f"/master/?message={len(item_ids)}개 항목이 삭제되었습니다.")


@csrf_exempt
@login_required
def master_bulk_dispose(request):
    if request.method != "POST":
        return redirect("master")
    item_ids = [int(item_id) for item_id in request.POST.getlist("item_ids") if str(item_id).strip()]
    for item_id in item_ids:
        dispose_reagent(item_id=item_id, reason="자동 폐기", disposal_type="MANUAL")
    return redirect(f"/master/?message={len(item_ids)}개 항목이 폐기되었습니다.")


@login_required
def download_master_upload_template(request):
    df = pd.DataFrame(
        [
            {
                "hazardous": "Y",
                "part": "TA",
                "item_code": "CRP001",
                "item_name": "CRP 시약",
                "lot_no": "*",
                "expiry_date": "20261231",
                "spec": "500mL",
                "unit": "EA",
                "reagent_type": "1",
                "equipment": "c702",
                "vendor": "Roche",
                "safety_stock": 10,
            }
        ]
    )
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="master_upload_template")
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = "attachment; filename=master_upload_template.xlsx"
    return response


@csrf_exempt
@login_required
def upload_master_preview(request):
    if request.method != "POST":
        return redirect("master")
    try:
        file = request.FILES["file"]
        filename = file.name.lower()
        if filename.endswith(".xlsx"):
            df = pd.read_excel(file, dtype=str)
        else:
            df = pd.read_csv(file, dtype=str)
        df = df.fillna("")
        preview_result = preview_bulk_master_items_v3(df)
        context = get_master_base_context()
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "duplicate_count": preview_result["duplicate_count"],
                "duplicate_names": preview_result["duplicate_names"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return render(request, "master.html", context)
    except Exception as exc:
        context = get_master_base_context()
        context["upload_error"] = str(exc)
        return render(request, "master.html", context)


@csrf_exempt
@login_required
def upload_master_confirm(request):
    if request.method != "POST":
        return redirect("master")
    try:
        rows = json.loads(request.POST.get("upload_data", "[]"))
        result = confirm_bulk_master_items(rows)
        context = get_master_base_context()
        context.update(
            {
                "upload_done": True,
                "total_uploaded": result["total"],
                "success_count": result["success"],
                "fail_count": result["fail"],
                "fail_messages": result["fail_messages"],
            }
        )
        return render(request, "master.html", context)
    except Exception as exc:
        context = get_master_base_context()
        context["upload_error"] = str(exc)
        return render(request, "master.html", context)


@login_required
def inbound_page(request):
    q = request.GET.get("q", "")
    part = _get_part(request)
    sort = request.GET.get("sort", "")
    order = request.GET.get("order", "")
    context = {
        "active_menu": "inbound",
        "tx_mode": "inbound",
        "tx_title": "시약 입고 등록",
        "tx_button_label": "입고 등록",
        "tx_qty_label": "입고수량",
        "tx_date_label": "입고일자",
        "message": request.GET.get("message", ""),
        "error": request.GET.get("error", ""),
        "q": q,
        "part": part,
        "sort": sort,
        "order": order,
        "part_map": get_part_map(get_current_schema()),
    }
    context.update(get_inbound_page_data(q=q, part=part, sort=sort, order=order))
    return render(request, "transaction_entry.html", context)


def get_inbound_base_context(q: str = "", part: str = "", sort: str = "", order: str = ""):
    context = {
        "active_menu": "inbound",
        "tx_mode": "inbound",
        "tx_title": "시약 입고 등록",
        "tx_button_label": "입고 등록",
        "tx_qty_label": "입고수량",
        "tx_date_label": "입고일자",
        "message": "",
        "error": "",
        "q": q,
        "part": part,
        "sort": sort,
        "order": order,
        "part_map": get_part_map(get_current_schema()),
    }
    context.update(get_inbound_page_data(q=q, part=part, sort=sort, order=order))
    return context


@csrf_exempt
@login_required
def inbound_bulk_create(request):
    if request.method != "POST":
        return redirect("inbound")
    try:
        rows = json.loads(request.POST.get("rows_json", "[]"))
    except json.JSONDecodeError:
        return redirect(f"/inbound/?error={quote_plus('선택된 항목 데이터를 읽을 수 없습니다.')}")

    ok, msg = create_bulk_inbound_transactions(rows)
    target = "message" if ok else "error"
    return redirect(f"/inbound/?{target}={quote_plus(msg)}")


@csrf_exempt
@login_required
def inbound_bulk_preview(request):
    if request.method != "POST":
        return redirect("inbound")
    q = request.POST.get("q", "")
    part = request.POST.get("part", "")
    try:
        rows = json.loads(request.POST.get("rows_json", "[]"))
        preview_result = preview_manual_inbound_items(rows)
        context = get_inbound_base_context(q=q, part=part)
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return render(request, "transaction_entry.html", context)
    except Exception as exc:
        context = get_inbound_base_context(q=q, part=part)
        context["error"] = str(exc)
        return render(request, "transaction_entry.html", context)


@login_required
def download_inbound_upload_template(request):
    df = pd.DataFrame(
        [{"item_code": "CRP001", "lot_no": "LOT202603", "qty": 5, "tx_date": "20260409"}]
    )
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="inbound_upload_template")
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = "attachment; filename=inbound_upload_template.xlsx"
    return response


@csrf_exempt
@login_required
def inbound_upload_preview(request):
    if request.method != "POST":
        return redirect("inbound")
    q = request.POST.get("q", "")
    part = request.POST.get("part", "")
    try:
        file = request.FILES["file"]
        filename = file.name.lower()
        if filename.endswith(".xlsx"):
            df = pd.read_excel(file, dtype=str)
        else:
            df = pd.read_csv(file, dtype=str)
        df = df.fillna("")
        preview_result = preview_bulk_inbound_items(df)
        context = get_inbound_base_context(q=q, part=part)
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return render(request, "transaction_entry.html", context)
    except Exception as exc:
        context = get_inbound_base_context(q=q, part=part)
        context["upload_error"] = str(exc)
        return render(request, "transaction_entry.html", context)


@csrf_exempt
@login_required
def inbound_upload_confirm(request):
    if request.method != "POST":
        return redirect("inbound")
    q = request.POST.get("q", "")
    part = request.POST.get("part", "")
    try:
        rows = json.loads(request.POST.get("upload_data", "[]"))
        ok, msg = create_bulk_inbound_transactions(rows)
        context = get_inbound_base_context(q=q, part=part)
        if ok:
            context["message"] = msg
        else:
            context["error"] = msg
        return render(request, "transaction_entry.html", context)
    except Exception as exc:
        context = get_inbound_base_context(q=q, part=part)
        context["upload_error"] = str(exc)
        return render(request, "transaction_entry.html", context)


@login_required
def outbound_page(request):
    q = request.GET.get("q", "")
    part = _get_part(request)
    sort = request.GET.get("sort", "")
    order = request.GET.get("order", "")
    context = {
        "active_menu": "outbound",
        "tx_mode": "outbound",
        "tx_title": "시약 출고 등록",
        "tx_button_label": "출고 등록",
        "tx_qty_label": "출고수량",
        "tx_date_label": "출고일자",
        "message": request.GET.get("message", ""),
        "error": request.GET.get("error", ""),
        "q": q,
        "part": part,
        "sort": sort,
        "order": order,
        "part_map": get_part_map(get_current_schema()),
    }
    context.update(get_outbound_page_data(q=q, part=part, sort=sort, order=order))
    return render(request, "transaction_entry.html", context)


def get_outbound_base_context(q: str = "", part: str = "", sort: str = "", order: str = ""):
    context = {
        "active_menu": "outbound",
        "tx_mode": "outbound",
        "tx_title": "시약 출고 등록",
        "tx_button_label": "출고 등록",
        "tx_qty_label": "출고수량",
        "tx_date_label": "출고일자",
        "message": "",
        "error": "",
        "q": q,
        "part": part,
        "sort": sort,
        "order": order,
        "part_map": get_part_map(get_current_schema()),
    }
    context.update(get_outbound_page_data(q=q, part=part, sort=sort, order=order))
    return context


@csrf_exempt
@login_required
def outbound_bulk_create(request):
    if request.method != "POST":
        return redirect("outbound")
    try:
        rows = json.loads(request.POST.get("rows_json", "[]"))
    except json.JSONDecodeError:
        return redirect(f"/outbound/?error={quote_plus('선택된 항목 데이터를 읽을 수 없습니다.')}")

    ok, msg = create_bulk_outbound_transactions(rows)
    target = "message" if ok else "error"
    return redirect(f"/outbound/?{target}={quote_plus(msg)}")


@csrf_exempt
@login_required
def outbound_bulk_preview(request):
    if request.method != "POST":
        return redirect("outbound")
    q = request.POST.get("q", "")
    part = request.POST.get("part", "")
    try:
        rows = json.loads(request.POST.get("rows_json", "[]"))
        preview_result = preview_manual_outbound_items(rows)
        context = get_outbound_base_context(q=q, part=part)
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return render(request, "transaction_entry.html", context)
    except Exception as exc:
        context = get_outbound_base_context(q=q, part=part)
        context["error"] = str(exc)
        return render(request, "transaction_entry.html", context)


@login_required
def download_outbound_upload_template(request):
    df = pd.DataFrame(
        [{"item_code": "CRP001", "lot_no": "LOT202603", "qty": 2, "tx_date": "20260409"}]
    )
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="outbound_upload_template")
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = "attachment; filename=outbound_upload_template.xlsx"
    return response


@csrf_exempt
@login_required
def outbound_upload_preview(request):
    if request.method != "POST":
        return redirect("outbound")
    q = request.POST.get("q", "")
    part = request.POST.get("part", "")
    try:
        file = request.FILES["file"]
        filename = file.name.lower()
        if filename.endswith(".xlsx"):
            df = pd.read_excel(file, dtype=str)
        else:
            df = pd.read_csv(file, dtype=str)
        df = df.fillna("")
        preview_result = preview_bulk_outbound_items(df)
        context = get_outbound_base_context(q=q, part=part)
        context.update(
            {
                "preview_mode": True,
                "preview_rows": preview_result["preview_rows"],
                "preview_json": json.dumps(preview_result["upload_rows"], ensure_ascii=False),
                "total_count": preview_result["total_count"],
                "valid_count": preview_result["valid_count"],
                "invalid_count": preview_result["invalid_count"],
                "invalid_messages": preview_result["invalid_messages"],
            }
        )
        return render(request, "transaction_entry.html", context)
    except Exception as exc:
        context = get_outbound_base_context(q=q, part=part)
        context["upload_error"] = str(exc)
        return render(request, "transaction_entry.html", context)


@csrf_exempt
@login_required
def outbound_upload_confirm(request):
    if request.method != "POST":
        return redirect("outbound")
    q = request.POST.get("q", "")
    part = request.POST.get("part", "")
    try:
        rows = json.loads(request.POST.get("upload_data", "[]"))
        ok, msg = create_bulk_outbound_transactions(rows)
        context = get_outbound_base_context(q=q, part=part)
        if ok:
            context["message"] = msg
        else:
            context["error"] = msg
        return render(request, "transaction_entry.html", context)
    except Exception as exc:
        context = get_outbound_base_context(q=q, part=part)
        context["upload_error"] = str(exc)
        return render(request, "transaction_entry.html", context)


@login_required
def history_page(request):
    from datetime import date, timedelta

    tx_type = request.GET.get("tx_type", "")
    part = _get_part(request)
    q = request.GET.get("q", "")
    date_from = request.GET.get("date_from", "")
    date_to = request.GET.get("date_to", "")
    period = request.GET.get("period", "7d")
    today = date.today()

    if not date_from and not date_to:
        if period == "1m":
            date_from = (today - timedelta(days=30)).isoformat()
        elif period == "6m":
            date_from = (today - timedelta(days=183)).isoformat()
        else:
            period = "7d"
            date_from = (today - timedelta(days=7)).isoformat()
        date_to = today.isoformat()

    context = {
        "active_menu": "history",
        "items": get_history_items(
            tx_type=tx_type,
            part=part,
            q=q,
            date_from=date_from,
            date_to=date_to,
        ),
        "tx_type": tx_type,
        "part": part,
        "q": q,
        "date_from": date_from,
        "date_to": date_to,
        "period": period,
        "part_map": get_part_map(get_current_schema()),
    }
    return render(request, "history.html", context)


@login_required
@user_passes_test(can_access_admin_area)
def history_admin_page(request):
    from datetime import date, timedelta

    tx_type = request.GET.get("tx_type", "")
    part = _get_part(request)
    q = request.GET.get("q", "")
    date_from = request.GET.get("date_from", "")
    date_to = request.GET.get("date_to", "")
    period = request.GET.get("period", "7d")
    today = date.today()

    if not date_from and not date_to:
        if period == "1m":
            date_from = (today - timedelta(days=30)).isoformat()
        elif period == "6m":
            date_from = (today - timedelta(days=183)).isoformat()
        else:
            period = "7d"
            date_from = (today - timedelta(days=7)).isoformat()
        date_to = today.isoformat()

    context = {
        "active_menu": "admin_panel",
        "items": get_history_items(tx_type=tx_type, part=part, q=q, date_from=date_from, date_to=date_to),
        "tx_type": tx_type,
        "part": part,
        "q": q,
        "date_from": date_from,
        "date_to": date_to,
        "period": period,
        "part_map": get_part_map(get_current_schema()),
        "message": request.GET.get("message", ""),
        "error": request.GET.get("error", ""),
    }
    return render(request, "history_admin.html", context)


@csrf_exempt
@login_required
@user_passes_test(can_access_admin_area)
def history_admin_edit(request, record_id):
    if request.method != "POST":
        return redirect("history_admin")
    tx_type = request.POST.get("tx_type", "").strip()
    tx_date = request.POST.get("tx_date", "").strip()
    qty = int(request.POST.get("qty", 0) or 0)
    if not tx_date or qty <= 0:
        return redirect("/history-admin/?error=거래일과 수량을 올바르게 입력해 주세요.")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE transaction_history SET tx_type = %s, tx_date = %s, qty = %s WHERE id = %s",
        (tx_type, tx_date, qty, record_id),
    )
    conn.commit()
    conn.close()
    return redirect("/history-admin/?message=수정되었습니다.")


@csrf_exempt
@login_required
@user_passes_test(can_access_admin_area)
def history_admin_delete(request, record_id):
    if request.method != "POST":
        return redirect("history_admin")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM transaction_history WHERE id = %s", (record_id,))
    conn.commit()
    conn.close()
    return redirect("/history-admin/?message=삭제되었습니다.")


@login_required
def reagent_history_page(request):
    part = _get_part(request)
    q = request.GET.get("q", "")
    sort = request.GET.get("sort", "")
    order = request.GET.get("order", "")
    reagent_type = request.GET.get("reagent_type", "")
    equipment = request.GET.get("equipment", "")
    vendor = request.GET.get("vendor", "")
    hazardous = request.GET.get("hazardous", "")
    disposed = request.GET.get("disposed", "")
    show_form = request.GET.get("show_form", "")
    selected_item_id = request.GET.get("selected_item_id", "")
    selected_item_label = request.GET.get("selected_item_label", "")
    selected_ids = request.GET.get("selected_ids", "")
    _forced = "" if _is_dlab(request) else "ZZ"
    old_new_part = _forced or request.GET.get("old_new_part", "")
    old_new_mode = request.GET.get("old_new_mode", "")
    manage_part = _forced or request.GET.get("manage_part", "")
    manage_mode = request.GET.get("manage_mode", "")

    effective_part = manage_part or part
    effective_q = "" if manage_part else q
    effective_reagent_type = "" if manage_part else reagent_type
    effective_equipment = "" if manage_part else equipment
    effective_vendor = "" if manage_part else vendor
    effective_hazardous = "" if manage_part else hazardous
    effective_disposed = "" if manage_part else disposed
    effective_lot_status = "NEW" if manage_mode == "new" else ""

    from datetime import datetime

    context = {
        "active_menu": "reagent_history",
        "items": get_reagent_history_items(
            part=effective_part,
            q=effective_q,
            reagent_type=effective_reagent_type,
            equipment=effective_equipment,
            vendor=effective_vendor,
            hazardous=effective_hazardous,
            disposed=effective_disposed,
            lot_status=effective_lot_status,
            sort=sort,
            order=order,
        ),
        "part_map": get_part_map(get_current_schema()),
        "part": part,
        "q": q,
        "sort": sort,
        "order": order,
        "message": request.GET.get("message", ""),
        "error": request.GET.get("error", ""),
        "reagent_type": reagent_type,
        "equipment": equipment,
        "vendor": vendor,
        "hazardous": hazardous,
        "disposed": disposed,
        "show_form": show_form,
        "selected_item_id": selected_item_id,
        "selected_item_label": selected_item_label,
        "selected_ids": selected_ids,
        "old_new_part": old_new_part,
        "old_new_mode": old_new_mode,
        "manage_part": manage_part,
        "manage_mode": manage_mode,
        "old_new_items": get_old_new_lot_items(part=old_new_part, only_new=(old_new_mode == "new")),
        "default_date": datetime.now().strftime("%Y-%m-%d"),
        "filter_options": get_reagent_history_filter_options(),
    }
    return render(request, "reagent_history.html", context)


@csrf_exempt
@login_required
def reagent_history_opened_at(request):
    if request.method != "POST":
        return redirect("reagent_history")

    item_id = int(request.POST.get("item_id", "0") or 0)
    opened_at = request.POST.get("opened_at", "")
    manage_part = request.POST.get("manage_part", "")
    manage_mode = request.POST.get("manage_mode", "")
    q = request.POST.get("q", "")
    sort = request.POST.get("sort", "")
    order = request.POST.get("order", "")

    ok, msg = update_opened_at(item_id=item_id, opened_at=opened_at)
    query = (
        f"?manage_part={quote_plus(manage_part)}&manage_mode={quote_plus(manage_mode)}"
        f"&q={quote_plus(q)}&sort={quote_plus(sort)}&order={quote_plus(order)}"
    )
    if ok:
        return redirect(f"/reagent-history/{query}&message={quote_plus('개봉 날짜가 등록되었습니다.')}")
    return redirect(f"/reagent-history/{query}&show_form=opened&error={quote_plus(msg)}")


@csrf_exempt
@login_required
def reagent_history_parallel_at(request):
    if request.method != "POST":
        return redirect("reagent_history")

    item_id = int(request.POST.get("item_id", "0") or 0)
    parallel_at = request.POST.get("parallel_at", "")
    manage_part = request.POST.get("manage_part", "")
    manage_mode = request.POST.get("manage_mode", "")
    q = request.POST.get("q", "")
    sort = request.POST.get("sort", "")
    order = request.POST.get("order", "")

    ok, msg = update_parallel_at(item_id=item_id, parallel_at=parallel_at)
    query = (
        f"?manage_part={quote_plus(manage_part)}&manage_mode={quote_plus(manage_mode)}"
        f"&q={quote_plus(q)}&sort={quote_plus(sort)}&order={quote_plus(order)}"
    )
    if ok:
        return redirect(f"/reagent-history/{query}&message={quote_plus('Parallel 날짜가 등록되었습니다.')}")
    return redirect(f"/reagent-history/{query}&show_form=parallel&error={quote_plus(msg)}")


@csrf_exempt
@login_required
def reagent_history_old_new_lot_save(request):
    if request.method != "POST":
        return redirect("reagent_history")

    part = request.POST.get("part", "")
    visible_item_ids = [int(item_id) for item_id in request.POST.getlist("visible_item_ids") if str(item_id).strip()]
    new_lot_item_ids = [int(item_id) for item_id in request.POST.getlist("new_lot_item_ids") if str(item_id).strip()]

    ok, msg = save_old_new_lot_selection(
        part=part,
        visible_item_ids=visible_item_ids,
        new_lot_item_ids=new_lot_item_ids,
    )
    target = f"/reagent-history/?old_new_part={quote_plus(part)}"
    if ok:
        return redirect(f"{target}&message={quote_plus(msg)}")
    return redirect(f"{target}&error={quote_plus(msg)}")
