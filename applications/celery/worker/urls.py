from django.urls import path

from . import views

urlpatterns = [
    path(
        "report/<int:report_id>/",
        views.ReportView.as_view(),
        name="report",
    ),
    path(
        "bulk-notify/",
        views.BulkNotifyView.as_view(),
        name="bulk-notify",
    ),
    path(
        "export/<int:export_id>/",
        views.ExportView.as_view(),
        name="export",
    ),
    path("status/", views.StatusView.as_view(), name="status"),
    path(
        "cache/<int:report_id>/",
        views.RedisCacheView.as_view(),
        name="redis-cache",
    ),
]
