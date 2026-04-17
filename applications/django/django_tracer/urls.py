from django.urls import path

from django_tracer.views import (
    AsyncView,
    DbView,
    IndexView,
    NPlusOneView,
    SlowView,
)

urlpatterns = [
    path("", IndexView.as_view()),
    path("async/", AsyncView.as_view()),
    path("slow/", SlowView.as_view()),
    path("db/", DbView.as_view()),
    path("n1/", NPlusOneView.as_view()),
]
