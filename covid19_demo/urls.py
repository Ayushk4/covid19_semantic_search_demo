"""covid19_demo URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from covid19.views import *

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/covid19/', get_queryset),
    path('covid19/', main_page),
    path('covid19/positive/', positive),
    path('covid19/negative/', negative),
    path('covid19/can_not_test/', can_not_test),
    path('covid19/death/', death),
    path('covid19/cure/', cure),
    ## examples
    # path('covid19/examples/positive/query1', positive_query_send_request),
    path('covid19/examples/demo_<str:demo_id>', demo_query_agg),
    path('covid19/show_complete_<str:input_query>', complete_query)
]
