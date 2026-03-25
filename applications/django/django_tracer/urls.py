"""django_tracer/urls.py"""

from django.urls import path

from . import views

urlpatterns = [
    path("", views.IndexView.as_view(), name="index"),
    path("async/", views.AsyncView.as_view(), name="async"),
    path("slow/", views.SlowView.as_view(), name="slow"),
    path("db/", views.DbView.as_view(), name="db"),
    path("n1/", views.NPlusOneView.as_view(), name="n1"),
]
