from django.urls import path

from django_tracer.views import (
    AsyncView,
    CascadeView,
    DbView,
    ExternalView,
    IndexView,
    NPlusOneView,
    PaymentView,
    RegressionView,
    SlowView,
)

urlpatterns = [
    path("", IndexView.as_view()),
    path("async/", AsyncView.as_view()),
    path("slow/", SlowView.as_view()),
    path("db/", DbView.as_view()),
    path("n1/", NPlusOneView.as_view()),
    path("cascade/", CascadeView.as_view()),
    path("regression/", RegressionView.as_view()),
    path("external/", ExternalView.as_view()),
    path("payment/", PaymentView.as_view()),
]
