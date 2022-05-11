import telegram
import pandahouse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import io

bot = telegram.Bot(token = "5373576465:AAFHbGFKlmy3bpRzH4vxZtCUZ-jc_KHWo5M")
chat_id = -691600284

def calculating_for_metrics(metrics, name_metrics):
    b = 0
    for j in metrics:
        window = 3
        line  = list(j)[-36:]
        L = int(window/2)
        MA = []
        for i in range(L, len(line)-L):
            if (i + L + 1) >= int(window / 2):
                c = sum(line[i-L:i+L+1])/ window
                MA.append(c)  
        sd = np.std(MA, ddof = 1)
        up = [(x + 2 * sd) for x in  MA]
        down = [(x - 2 * sd) for x in  MA]
        s = len(MA)

        if j.iloc[-1] > up[-1] or j.iloc[-1] < down[-1]:
            deviation = round(abs(round(1 - (j.iloc[-1]/j.iloc[-2]), 2) * 100),2)
            metric = j.iloc[-1]
            mes = f'Метрика: {name_metrics[b]}. Текущее значение: {metric}. Отклонение от предыдущего значения: {deviation} %.'

            bot.sendMessage(chat_id=chat_id, text = mes)
            line1 = line[-len(MA):]
            fig, ax = plt.subplots(figsize =(10, 7))
            hmin = list(df["hmin"])[-len(MA):]
            ax.plot(hmin, line1, label = name_metrics[b]) 
            ax.plot(hmin, MA, c = "g", label = "скользящее среднее")
            up = [(x + 2 * sd) for x in  MA]
            down = [(x - 2 * sd) for x in  MA]
            ax.fill_between(hmin, MA, up, facecolor='green',alpha = 0.23,color = 'green', linewidth = 2, label = "скользящее среднее ±2σ")   
            ax.fill_between(hmin, MA, down, facecolor='green',alpha = 0.23,color = 'green', linewidth = 2)
            ax.tick_params(axis='x', labelrotation=45)
            ax.set_xticks(range(0, len(hmin), 2))
            ax.grid()
            ax.legend()
            plot_object = io.BytesIO()
            fig.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'alert_plot.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            plt.show()
        b += 1

        
#table1 feed
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}
q = """
SELECT toStartOfFifteenMinutes(time) as tfif, 
        toDate(time) as dat,
        formatDateTime(tfif, '%R') as hmin,
        uniqExact(user_id) as user,
        countIf(user_id, action='view') as views,
        countIf(user_id, action='like') as likes,
        ROUND(countIf(user_id, action = 'like') / countIf(user_id, action = 'view'),2) AS CTR
FROM simulator_20220420.feed_actions
WHERE tfif >= today()-1 and tfif < toStartOfFifteenMinutes(now())
GROUP BY tfif, dat, hmin
ORDER BY tfif
"""

df = pandahouse.read_clickhouse(q, connection=connection)

metrics = [df["user"], df["views"], df["likes"], df["CTR"]]
name_metrics = ["количество уникальных пользователей ленты", "количество просмотров", "количество лайков", "CTR"]
calculating_for_metrics(metrics, name_metrics)

#table2 messeges
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}
q = """
SELECT toStartOfFifteenMinutes(time) as tfif, 
        toDate(time) as dat,
        formatDateTime(tfif, '%R') as hmin,
        uniqExact(user_id) as user,
        COUNT(reciever_id) as messages
FROM simulator_20220420.message_actions
WHERE tfif >= today()-1 and tfif < toStartOfFifteenMinutes(now())
GROUP BY tfif, dat, hmin
ORDER BY tfif  
"""

df = pandahouse.read_clickhouse(q, connection=connection)
metrics = [df["user"], df["messages"]]
name_metrics = ["количество уникальных пользователей мессенджера", "количество сообщений"]
calculating_for_metrics(metrics, name_metrics)
