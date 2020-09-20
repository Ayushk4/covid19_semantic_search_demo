from django.shortcuts import render
from django.views.generic import TemplateView, ListView
from .search import *


def get_queryset(request):
    q = request.GET.get('q')
    # print(q)
    if q is not None:
        # print('1st', q)
        return search_json(q)
    return super().get_queryset()


def main_page(request):
    return render(request, 'events/main.html')


def positive(request):
    return render(request, 'events/positive.html')


def negative(request):
    return render(request, 'events/negative.html')


def can_not_test(request):
    return render(request, 'events/can_not_test.html')


def death(request):
    return render(request, 'events/death.html')


def cure(request):
    return render(request, 'events/cure.html')


### examples
def positive_query_send_request(request):

    # trail_data = search("positive;*;;;Boris Johnson;;;2020-01-15;2020-08-26")
    # print('HERE', trail_data, trail_data.data)
    # trail_data =

    return render(request, 'example_queries/positive_example-OLD.html')


### examples
def demo_query_agg(request, demo_id):

    demo_res = None
    if demo_id == 'positive_1':
        demo_res = search_direct("positive;*;;;;;;;;2020-01-15;2020-08-26;100")
    if demo_id == 'positive_2':
        demo_res = search_direct("positive;;;*;;;;;;2020-01-15;2020-08-26;100")
    if demo_id == 'positive_3':
        demo_res = search_direct("positive;*;;;Boris Johnson;;;;;2020-01-15;2020-08-26;100")
    if demo_id == 'negative_1':
        demo_res = search_direct("negative;*;;;;;;;2020-01-15;2020-08-26;100")
    if demo_id == 'can_not_test_1':
        demo_res = search_direct("can_not_test;*;;;;Yes;2020-01-15;2020-08-26;100")
    if demo_id == 'death_1':
        demo_res = search_direct("death;*;;;;;2020-01-15;2020-08-26;100")
    if demo_id == 'cure_1':
        demo_res = search_direct("cure;*;;Yes;2020-01-15;2020-08-26;100")

    return render(request, 'example_queries/demo_agg_view.html', context={'data': demo_res})


def complete_query(request, input_query):

    complete_res = None
    demo_res = search_direct(input_query)[0]['data']

    return render(request, 'example_queries/complete_view.html', context={'data': demo_res})
