from django.urls import path
from . import views

urlpatterns = [
    path("report/<int:report_id>/", views.ReportView.as_view(),    name="report"),
    path("bulk-notify/",            views.BulkNotifyView.as_view(), name="bulk-notify"),
    path("export/<int:export_id>/", views.ExportView.as_view(),    name="export"),
    path("failing/",                views.FailingJobView.as_view(), name="failing"),
    path("status/",                 views.StatusView.as_view(),     name="status"),
]