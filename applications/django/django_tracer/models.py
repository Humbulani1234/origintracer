from django.db import models


class Author(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        app_label = "django_tracer"


class Book(models.Model):
    title = models.CharField(max_length=200)
    author = models.ForeignKey(Author, on_delete=models.CASCADE)

    class Meta:
        app_label = "django_tracer"


class Payment(models.Model):
    amount = models.IntegerField()
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = "django_tracer"
