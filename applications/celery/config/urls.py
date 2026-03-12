from django.urls import path, include

urlpatterns = [
    path("tasks/", include("worker.urls")),
]