#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
 * Created by Truan Wang on 2018/06/06
 *
 * Copyright ? 2014-2020 . 上海进馨网络科技有限公司 . All Rights Reserved
"""
import os
import sys

assert os.getenv("NAMIBOX_RUNNING_ENVIRON") is not None, \
    u"environment variable NAMIBOX_RUNNING_ENVIRON must be set explicitly!"
assert hasattr(sys, "NAMIBOX"), \
    u"sys.NAMIBOX not been set yet! Please make sure jct-site/_platform_/ has been import."

# 添加 justing/namibox/tina3 到搜索路径
sys.path.insert(0, os.path.join(sys.NAMIBOX, "tina3"))

# 设置django settings 模块为 justing/namibox/tina3/settings.py
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")

from django.conf import settings

# 不使用django的日志系统
settings.LOGGING['disable_existing_loggers'] = False

from django.db import transaction
from message_workers import process_message_main, get_logger
from vschool.models import VirtualSchoolClassSchedule, VirtualSchoolUserProfile, VirtualSchoolGraduationCertificate, \
    VirtualSchoolClassMember, VirtualSchoolUserAchieve, VirtualSchoolGraduationAnswer, VirtualSchoolClass
from appnative.models import Jct5PmeUserData
from lesson.models import Jct4MilessonItem, Jct6MilessonItemScoreExtend
from auth.models import User, Jct2UserProfile
from message_workers import send_message, NamiboxMessage
from message_workers.configure_settings import auto_detected_run_location
import json_py26
import datetime
import traceback
from django.db.models import F

logger = get_logger("notify-school-queue")

def get_site():
    en_site = 'https://' + settings.DOMAIN
    return en_site


@transaction.commit_on_success(using="default")
def school_task(user_id, vs_class_id, task_type):
    """
    学员完成作业任务和一次性任务后重新计算jct5_pme_user_data表的作业数据
    :param user_id:
    :param vs_class_id:
    :param task_type:
    :return:
    """
    now = datetime.datetime.now()

    # 一次性任务
    class_schedule = list(VirtualSchoolClassSchedule.objects.filter(vs_class_id=vs_class_id).order_by('start_at'))
    if task_type == "one_task":
        one_task_credit = get_onetask_credit(user_id, vs_class_id,class_schedule,now)
        Jct5PmeUserData.objects.filter(user_id=user_id, obj_type='vschool', obj_id=vs_class_id).update(
            one_task_credit=one_task_credit, last_modify_time=now)
        return True

    elif task_type == "works_task":
        task_credit =  get_works_task_credit(user_id, vs_class_id, now,class_schedule)
        if task_credit:
            Jct5PmeUserData.objects.filter(user_id=user_id, obj_type='vschool', obj_id=vs_class_id).update(
                pre_homework_credit=task_credit[0], homework_credit=task_credit[1], last_modify_time=now)
            return True
        return False

    elif task_type == "all_task":
        one_task_credit = get_onetask_credit(user_id, vs_class_id,class_schedule,now)
        task_credit = get_works_task_credit(user_id, vs_class_id, now,class_schedule)
        befor_work_credit = 0
        after_work_credit = 0
        if task_credit:
            befor_work_credit = task_credit[0]
            after_work_credit = task_credit[1]

        Jct5PmeUserData.objects.filter(user_id=user_id, obj_type='vschool', obj_id=vs_class_id).update(one_task_credit=one_task_credit,
            pre_homework_credit=befor_work_credit, homework_credit=after_work_credit, last_modify_time=now)
        return True


@transaction.commit_on_success(using="default")
def school_living_end(vs_class_id,lesson_id):
    """
    班级直播结束的通知
    :param vs_class_id:
    :return:
    """
    user_list = []
    class_schedule = list(VirtualSchoolClassSchedule.objects.filter(vs_class_id=vs_class_id).order_by('start_at'))
    vs_class_obj = VirtualSchoolClass.objects.get(id=vs_class_id)
    student_members = list(VirtualSchoolClassMember.objects.filter(vs_class_id=vs_class_id, role=u'学生', is_active=1).order_by("-class_score").values("user_id", "class_score"))
    student_ids = [mem["user_id"] for mem in student_members]
    jct2_users = list(
        Jct2UserProfile.objects.filter(user_id__in=student_ids).values("user_id", "ec_name", "nick_name"))
    auth_users = list(User.objects.filter(id__in=student_ids).values("id", "username"))

    student_profiles = list(
        VirtualSchoolUserProfile.objects.filter(user_id__in=student_ids).values("user_id", "headimg"))

    student_info_list = []
    rank_index = 0
    for mem in student_members:
        rank_index += 1
        user_info_dict = {"user_id": mem["user_id"], "class_score": mem["class_score"], "rank_in_class": rank_index}
        for pro in jct2_users:
            if pro["user_id"] == mem["user_id"]:
                user_info_dict["nick_name"] = pro["ec_name"] if pro["ec_name"] else pro["nick_name"]
                break

        for stu in student_profiles:
            if stu["user_id"] == mem["user_id"]:
                user_info_dict["head_image"] = stu["headimg"] if stu.get("headimg") else ""
                break

        for au in auth_users:
            if au["id"] == mem["user_id"]:
                user_info_dict["username"] = au["username"]
                break

        student_info_list.append(user_info_dict)

    now = datetime.datetime.now()
    net_id = 0
    next_lesson_starttime = None
    is_last_lesson = False
    for index, course in enumerate(class_schedule):
        # 最后一节课
        if index == len(class_schedule) - 1 and course.real_end_at:
            is_last_lesson = True
            break

        # 下一节课时间
        if course.start_at > now and not course.real_end_at:
            net_id = course.milesson_item_id
            next_lesson_starttime = course.start_at
            break

    if not is_last_lesson:
        milessonitem = list(Jct4MilessonItem.objects.filter(id=net_id))

        if milessonitem :
            arg = {
                'next_lesson_id': milessonitem[0].id,
                'next_lesson_name': milessonitem[0].text,
                'next_lesson_starttime': next_lesson_starttime,
                'end_lesson_period':index,# 每次最新不需要加1
                'msg_count':F('msg_count') + 1,
            }

            Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).update(
                is_living=0, last_modify_time=now, **arg)

            user_list = list(
                Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).values_list('user_id', flat=True))

    elif is_last_lesson and len(class_schedule) > 1:  # 毕业相关消息通知---2018-10-15penghantian
        # 更新中间表状态
        Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).update(
            is_living=0, last_modify_time=now, end_lesson_period=index + 1, state=u'毕业考试' , msg_count=F('msg_count') + 1)

        user_list = list(
            Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).values_list('user_id', flat=True))

        # 处理毕业证书信息
        bulk_create_cer = []
        for user_dict in student_info_list:
            user_name = user_dict["nick_name"] or user_dict["username"]
            vs_cer_obj = VirtualSchoolGraduationCertificate(vs_class_id=vs_class_id, class_name=vs_class_obj.name, user_id=user_dict["user_id"],
                                                            user_name=user_name, head_image=user_dict.get("head_image", ""),
                                                            class_score=user_dict["class_score"], rank_in_class=user_dict["rank_in_class"], create_time=now)
            bulk_create_cer.append(vs_cer_obj)
        VirtualSchoolGraduationCertificate.objects.bulk_create(bulk_create_cer)

        # 处理毕业考试或毕业反馈动作未完成推送
        if auto_detected_run_location == "ecs":
            three_day_end = now + datetime.timedelta(days=3)  # 最后一节课结束后73小时
            six_day_end = now + datetime.timedelta(days=6)  # 毕业考试最长时间7天，毕业考试结束前24小时
        else:
            three_day_end = now + datetime.timedelta(minutes=6)  # 最后一节课结束后73小时
            six_day_end = now + datetime.timedelta(minutes=10)  # 毕业考试最长时间7天，毕业考试结束前24小时

        graduation_message1 = NamiboxMessage(
            "notify-school-graduation",
            vs_class_id=vs_class_id,
            class_name=vs_class_obj.name,
            student_ids=student_ids,
            student_info_list=student_info_list,
            three_day_end=three_day_end,
            six_day_end=six_day_end,
            repeat_schedule=[three_day_end]
        )

        graduation_message2 = NamiboxMessage(
            "notify-school-graduation",
            vs_class_id=vs_class_id,
            class_name=vs_class_obj.name,
            student_ids=student_ids,
            student_info_list=student_info_list,
            three_day_end=three_day_end,
            six_day_end=six_day_end,
            repeat_schedule=[six_day_end]
        )
        # todo: 针对很多个班级同时毕业，方案：可以随机延迟几秒钟推送(未做)

        send_message(queue_name="notify-school-queue", message=graduation_message1)
        send_message(queue_name="notify-school-queue", message=graduation_message2)

    else:  # 试听课---不需要做毕业相关处理
        Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).update(
            is_living=0, last_modify_time=now, end_lesson_period=index + 1, msg_count=F('msg_count') + 1)

        user_list = list(
            Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).values_list('user_id', flat=True))
    return user_list


@transaction.commit_on_success(using="default")
def school_living(vs_class_id):
    """
    班级直播开始通知
    :param vs_class_id:
    :return:
    """
    user_list = []
    class_schedule = list(VirtualSchoolClassSchedule.objects.filter(vs_class_id=vs_class_id).order_by('start_at'))
    now = datetime.datetime.now()

    show_courses = {}

    for index, course in enumerate(class_schedule):
        # 直播过程中不存在任何课前和课后任务
        if course.start_at - datetime.timedelta(minutes=20) <= now and not course.real_end_at and now < course.start_at + datetime.timedelta(minutes=60):
            show_courses[course.milesson_item_id] = 1
            if index < len(class_schedule) - 1:
                show_courses[class_schedule[index + 1].milesson_item_id] = 0
            break

    # 查询下一节课的数据
    if show_courses:
        lessons = list(Jct4MilessonItem.objects.filter(id__in=show_courses.keys()))
        milesson_id = lessons[0].milesson_id
        milessonitem = list(Jct4MilessonItem.objects.filter(milesson_id=milesson_id))

        living_id = 0
        after_work_id = 0
        befor_work_id = 0
        after_ids = []
        befor_ids = []
        for item in milessonitem:
            if item.parent_item_id in show_courses.keys():
                if show_courses[item.parent_item_id] == 0:
                    if item.text == u'课前作业':
                        befor_work_id = item.id
                if show_courses[item.parent_item_id] == 1:
                    if item.text == u'课后作业':
                        after_work_id = item.id
                    elif item.text == u'课程视频':
                        living_id = item.id

        for w_item in milessonitem:
            if w_item.parent_item_id == after_work_id:
                after_ids.append(w_item.id)
            elif w_item.parent_item_id == befor_work_id:
                befor_ids.append(w_item.id)

        w_score_ids = after_ids + befor_ids + [living_id]
        scores = list(Jct6MilessonItemScoreExtend.objects.filter(milesson_item_id__in=w_score_ids))

        living_credit = 0
        homework_credit = 0
        pre_homework_credit = 0

        for score in scores:
            if score.milesson_item_id == living_id:
                living_credit = score.total_score
            elif score.milesson_item_id in after_ids:
                homework_credit += score.total_score
            elif score.milesson_item_id in befor_ids:
                pre_homework_credit += score.total_score

        Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).update(
            is_living=1, living_credit=living_credit, pre_homework_credit=pre_homework_credit,
            homework_credit=homework_credit, last_modify_time=now)

        user_list = list(
            Jct5PmeUserData.objects.filter(obj_type='vschool', obj_id=vs_class_id).values_list('user_id',
                                                                                               flat=True))
    return user_list


@transaction.commit_on_success(using="default")
def get_onetask_credit(user_id , vs_class_id,class_schedule,now):
    """
    一次性任务学分
    :param user_id:
    :param vs_class_id:
    :return:
    """

    is_three_finish = False
    for index, course in enumerate(class_schedule):
        course.index = index + 1
        if index == 2 and course.real_end_at and now >= course.real_end_at:
            is_three_finish = True
            break
    one_tasks = [('bingwx', 50), ('sign', 5), ('headimg', 5), ('course', 20)]
    if is_three_finish:
        teachers = list(
            VirtualSchoolClassMember.objects.filter(vs_class_id=vs_class_id, role__in=[u'人工助教', u'AI助教'],
                                                    is_active=1))

        teacher_ids = dict((teacher.user_id, teacher.role) for teacher in teachers)
        list_course = [schedule.milesson_item_id for schedule in class_schedule]
        vs_teachers = list(
            Jct4MilessonItem.objects.filter(id__in=list_course, teacher__isnull=False).values_list(
                'teacher_id', flat=True).distinct())

        for vs_teacher in vs_teachers:
            teacher_ids[vs_teacher]=u'讲师'

        one_tasks.append(('ev_allteacher', len(teacher_ids) * 20 - 10))
        one_tasks.append(('ev_course', 20))

    vs_user_achieves = list(VirtualSchoolUserAchieve.objects.filter(vs_class_id=vs_class_id, user_id=user_id,
                                                               receive_time__isnull=False,
                                                               obj_type__in=['bingwx', 'sign', 'headimg', 'face',
                                                                             'course', 'ev_course', 'ev_teacher',
                                                                             'ev_assistant', 'ev_ai']))
    achieve_types = []
    ev_achieve = {}
    for achieve in vs_user_achieves:
        if achieve.obj_type in ('ev_ai', 'ev_teacher', 'ev_assistant'):
            ev_achieve[achieve.obj_id] = achieve.obj_type
        else:
            achieve_types.append(achieve.obj_type)

    one_task_credit = 0
    for one_task in one_tasks:
        if one_task[0] == "ev_allteacher":
            for teacher_id, _types in teacher_ids.items():
                if teacher_id not in ev_achieve.keys():
                    if _types == u'AI助教':
                        one_task_credit += 10
                    else:
                        one_task_credit += 20

        elif one_task[0] != "ev_allteacher" and one_task[0] not in achieve_types:
            one_task_credit += one_task[1]

    return one_task_credit

def get_works_task_credit(user_id,vs_class_id,now,class_schedule):
    """
    作业任务学分，课前和课后作业
    :param user_id:
    :param vs_class_id:
    :param now:
    :return:
    """
    show_courses = {}
    for index, course in enumerate(class_schedule):
        # 第一节课只显示课前任务
        if index == 0 and (now < course.start_at):
            show_courses[course.milesson_item_id] = 0

        # 直播过程中不存在任何课前和课后任务
        if course.start_at <= now and not course.real_end_at:
            break

        # 最后一节课只先显示课后任务
        if index == len(class_schedule) - 1 and course.real_end_at and now >= course.real_end_at:
            show_courses[course.milesson_item_id] = 1
            break

        # 显示最新结束的一节课程课后任务以及即将上课的一节课程课前任务
        if course.real_end_at and course.real_end_at <= now < class_schedule[index + 1].start_at:
            show_courses[course.milesson_item_id] = 1
            show_courses[class_schedule[index + 1].milesson_item_id] = 0
            break

    if show_courses:
        milessonitem = list(
            Jct4MilessonItem.objects.filter(parent_item_id__in=show_courses.keys(), text__in=[u'课前作业', u'课后作业']))

        work_scores = []
        work_parent = {}
        for work_item in milessonitem:
            if show_courses[work_item.parent_item_id] == 0 and work_item.text == u'课前作业':
                work_parent[work_item.id] = 0
            elif show_courses[work_item.parent_item_id] == 1 and work_item.text == u'课后作业':
                work_parent[work_item.id] = 1

        if work_parent:
            vs_sql = """SELECT a.id,a.parent_item_id,b.total_score FROM jct4_milesson_item a,jct6_milesson_item_score_extend b
                                WHERE a.parent_item_id in (%s) AND a.id = b.milesson_item_id""" % ','.join(
                str(i) for i in work_parent.keys())
            work_scores = list(Jct4MilessonItem.objects.raw(vs_sql))

        if work_scores:
            vs_user_achieves = VirtualSchoolUserAchieve.objects.filter(vs_class_id=vs_class_id, user_id=user_id,
                                                                       receive_time__isnull=False,
                                                                       obj_type=u'works')
            user_achieve = [achieve.obj_id for achieve in vs_user_achieves]
            before_score = 0
            after_score = 0
            for work_score in work_scores:
                if work_score.id not in user_achieve:
                    if work_parent[work_score.parent_item_id] == 0:
                        before_score += work_score.total_score
                    elif work_parent[work_score.parent_item_id] == 1:
                        after_score += work_score.total_score
            return  before_score , after_score
    return None


def apppush_vschool(user_list):
    app_push_message = NamiboxMessage(
        "base-jpush",
        user_id_list=user_list,
        content={
            "type": "",
            "message_name": "",
            "message": json_py26.dumps(
                {"command": "message", "message": "refresh_lanmu", "obj": {'id': 'pme_vschool'}}
            ),
            "dest_view_name": "main_me",
        }
    )
    # 直接发送给极光队列处理
    send_message(queue_name="base-jpush-queue", message=app_push_message)


@transaction.commit_on_success(using="default")
def handle_graduation(vs_class_id, student_ids, class_name, student_info_list, three_day_end, six_day_end):
    now_time = datetime.datetime.now()
    finish_feedback_ids = VirtualSchoolGraduationAnswer.objects.filter(user_id__in=student_ids, vs_class_id=vs_class_id).values_list("user_id", flat=True)
    if finish_feedback_ids:
        finish_feedback_and_test_ids = VirtualSchoolUserAchieve.objects.filter(vs_class_id=vs_class_id, user_id__in=finish_feedback_ids, obj_type="graduation").values_list("user_id", flat=True)
        need_push_user_ids = list(set(student_ids)^set(finish_feedback_and_test_ids))  # 差集
        need_push_user = []
        for stu in student_info_list:
            for ns in need_push_user_ids:
                if ns == stu["user_id"]:
                    need_push_user.append(stu)

    else:
        need_push_user_ids = student_ids
        need_push_user = student_info_list

    logger.info(u"----当前时间为%s,未完成毕业两个动作的人%s----"%(now_time, need_push_user_ids))

    if need_push_user_ids:
        if three_day_end <= now_time < six_day_end: # 三天后未完成，发离线消息
            # 推送notification离线消息
            params = {
                "user_id_list": need_push_user_ids,
                "push_platform": 'all',
                "expire_time": 259200,
                "content": {
                    'type': '',
                    'title': u'毕业考试提醒',
                    'content': u"亲爱的同学，您在纳米盒网校%s班级中还没有完成毕业考试，顺利毕业可以获得专属毕业证书，请尽快前往班级中完成哦！"%(class_name),
                    'url': '',
                    "hide": False,
                    "sub_type": 1,
                    'actions': [
                        {
                            "name": u"我知道了",
                            "action": {
                                "url": '%s/vschool/class/%s'%(get_site(), vs_class_id),
                                "command": "openview"
                            }
                        }
                    ],
                }
            }
            app_push_message = NamiboxMessage("base-jpush", **params)
            send_message(queue_name="base-jpush-queue", message=app_push_message)

        if six_day_end <= now_time <= six_day_end + datetime.timedelta(days=1):   # 六天后，发送短信消息
            # 短信提醒
            for user_dict in need_push_user:
                user_name = user_dict["nick_name"] or user_dict["username"]
                app_sms_message = NamiboxMessage(
                    "sms",
                    mobile_phone=user_dict["username"],
                    msg=u"【纳米盒】亲爱的%s同学，您在纳米盒网校%s班级中还没有完成毕业考试，顺利毕业可以获得专属毕业证书，请尽快打开纳米盒APP前往班级中完成哦！" % (user_name, class_name)
                )
                send_message(queue_name="base-sms-queue", message=app_sms_message)





def process_message(namibox_message):
    if namibox_message.type_ == "notify-school-task":
        user_id = namibox_message.data.get('user_id')
        vs_class_id = namibox_message.data.get('vs_class_id')
        now_time = namibox_message.data.get('now_time')
        task_type = namibox_message.data.get('task_type')
        if not user_id or not vs_class_id or not now_time or not task_type:
            logger.warn(u"消息【%s】参数不合法" % str(namibox_message))
        else:
            try:
                if school_task(user_id, vs_class_id, task_type):
                    apppush_vschool([user_id])
            except:
                logger.error(u"处理错误,%s" % str(traceback.format_exc()))
        return None

    elif namibox_message.type_ == "notify-school-living":
        vs_class_id = namibox_message.data.get('vs_class_id')
        try:
            user_list = school_living(vs_class_id)
            if user_list:
                apppush_vschool(user_list)
        except:
            logger.error(u"处理错误,%s" % str(traceback.format_exc()))

    elif namibox_message.type_ == "notify-school-living-end":
        vs_class_id = namibox_message.data.get('vs_class_id')
        lesson_id   = namibox_message.data.get('lesson_id')
        try:
            user_list = school_living_end(vs_class_id,lesson_id)
            if user_list:
                apppush_vschool(user_list)
        except:
            logger.error(u"处理错误,%s" % str(traceback.format_exc()))

    elif namibox_message.type_ == "notify-school-graduation":
        vs_class_id = namibox_message.data.get("vs_class_id")
        student_ids = namibox_message.data.get("student_ids")
        class_name = namibox_message.data.get("class_name")
        student_info_list = namibox_message.data.get("student_info_list")
        three_day_end = namibox_message.data.get("three_day_end")
        six_day_end = namibox_message.data.get("six_day_end")

        try:
            handle_graduation(vs_class_id, student_ids, class_name, student_info_list, three_day_end, six_day_end)
        except:
            logger.error(u'处理错误，%s'%(str(traceback.format_exc())))

    else:
        return namibox_message


if __name__ == "__main__":
    #school_living_end(80,123)
    # queue_message = NamiboxMessage("notify-school-living-end",
    #                                vs_class_id=1, lesson_id=1010)
    # send_message(queue_name="notify-school-queue", message=queue_message)
    # params = {
    #     "user_id_list": [12996425],
    #     "push_platform": 'all',
    #     "expire_time": 259200,
    #     "content": {
    #         'type': '',
    #         'title': u'毕业考试提醒',
    #         'content': u"亲爱的同学，您在纳米盒网校%s班级中还没有完成毕业考试，顺利毕业可以获得专属毕业证书，请尽快前往班级中完成哦！" % (u'dasadssad'),
    #         'url': '',
    #         "hide": False,
    #         "sub_type": 1,
    #         'actions': [
    #             {
    #                 "name": u"我知道了",
    #                 "action": {
    #                     "url": '%s/vschool/class/%s' % (get_site(), 1),
    #                     "command": "openview"
    #                 }
    #             }
    #         ],
    #     }
    # }
    # logger.info(u"发送离线消息%s" % (params))
    # app_push_message = NamiboxMessage("base-jpush", **params)
    # send_message(queue_name="base-jpush-queue", message=app_push_message)
    process_message_main("notify-school-queue", process_message)
