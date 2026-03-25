from django.urls import include, path

urlpatterns = [
    path("tasks/", include("worker.urls")),
]
