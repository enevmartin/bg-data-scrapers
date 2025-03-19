# apps/scrapers/models.py
from django.db import models

class ScrapedFile(models.Model):
    institution = models.CharField(max_length=100)
    original_url = models.URLField()
    file_path = models.CharField(max_length=255)
    file_name = models.CharField(max_length=255)
    file_size = models.PositiveIntegerField()
    mime_type = models.CharField(max_length=100)
    hash_value = models.CharField(max_length=64)
    date_created = models.DateTimeField(auto_now_add=True)
    last_updated = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('institution', 'original_url')
        indexes = [
            models.Index(fields=['institution']),
            models.Index(fields=['hash_value']),
        ]