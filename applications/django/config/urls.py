"""config/urls.py"""

from django.urls import include, path

urlpatterns = [
    path("", include("django_tracer.urls")),
]
