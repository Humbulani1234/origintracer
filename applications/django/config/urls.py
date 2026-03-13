"""config/urls.py"""

from django.urls import path, include

urlpatterns = [
    path("", include("django_tracer.urls")),
]
