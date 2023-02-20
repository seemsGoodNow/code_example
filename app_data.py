import sqlite3
import datetime
import os

import pandas as pd
import numpy as np

from CONSTANTS import CONST
import global_vars as global_vars

con = sqlite3.connect('../../databases/data', check_same_thread=False,
                      timeout=10)


class DataTables:
    """
    Class is parent class for all tabs, connected with Supplies (not UGS)
    (SupplyTime, SupplyCompare, DemandTime, etc.)

    Attributes:

    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    - :class:`DataTables` groupby: str, the same as in init params
    - :class: `DataTables` date_type: str, the same as in init params
    """

    def _set_start_date(self, start_date: pd.Timestamp) -> None:
        """
        Method sets _start_date parameter to instance of the class. Method
        is needed for Time classes (SupplyTime ...)
            :param start_date: pd.Timestamp
        """
        if start_date is not None:
            if isinstance(start_date, str):
                self._start_date = pd.Timestamp(start_date)
            else:
                self._start_date = start_date
        else:
            self._start_date = datetime.datetime.now() - pd.DateOffset(
                months=1) * CONST.MONTH_TO_SHOW,

    def _set_end_date(self, end_date: pd.Timestamp) -> None:
        """
        Method sets _end_date parameter to instance of the class. Method
        is needed for Time classes (SupplyTime ...)
            :param end_date: pd.Timestamp
        """
        if end_date is not None:
            if isinstance(end_date, str):
                self._end_date = pd.Timestamp(end_date)
            else:
                self._end_date = end_date
        else:
            self._end_date = CONST.TODAY

    def _set_compare_years(self, compare_years: list) -> None:
        """
        Method sets default value of compare_years if it is None
            :param compare_years: list of integers
        """
        if compare_years is None:
            self._compare_years = CONST.COMPARE_YEARS
        elif isinstance(compare_years, list):
            self._compare_years = compare_years
        else:
            self._compare_years = []

    @staticmethod
    def _set_countries(countries: list) -> list:
        """
        Method defines countries list if it is None
            :param countries: list of str, names of countries
        """
        # Check errors from dash, when dropdown returns string instead of list
        if not isinstance(countries, list):
            if countries is not None:
                countries = [countries]
            else:
                countries = []
        return countries

    def __define_divider(self, measure='millions') -> None:
        """
        Method defines divider, divider_label,
        axis_label and ru_div_label, based on parameter measure
            :param measure: str, volume types, one of 'millions', 'billions'
        """

        if measure is not None:
            if measure == 'billions':
                self.divider = 1000
            elif measure == 'millions':
                self.divider = 1
        else:
            self.divider = 1

    def __init__(self, measure: str, groupby: str, date_type: str):
        """
        :param measure: str, one of 'millions', 'billions'
        :param groupby: str, one of 'default', 'country', 'sum'.
             Characteristics of grouping lines on the graph:
            'default' means group by certain point,
            'country' means group by country-exporter or country-importer,
        :param date_type: str, one of 'День', 'Неделя',
                'Месяц', 'Год'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
        """
        self.__define_divider(measure)
        self.groupby = groupby
        self.date_type = date_type


class SupplyTime(DataTables):
    """
    Class is associated with the 'Валовые поставки'
    and 'Нетто поставки' tabs
    and generates data which is using
    in creating 'Валовые поставки'
    and 'Нетто поставки' tabs graphs (SupplyTime)

    Attributes:

    - :class:`SupplyTime` _start_date: pd.Timestamp, same as in Init params
    - :class:`SupplyTime` _end_date: pd.Timestamp, same as in Init params
    - :class:`SupplyTime` __exp_or_imp_grouby: str, one of 'country_from',
        'country_to', used in this and SupplyTimeFig classes in case when
        groupby = 'country' to define type of sql group by (e.g. chosen only
        exporter => exp_or_imp_groupby should be 'country_to', because we are
        interested in directions of export)
    - :class:`DataTables` date_type: str, same as init
    - :class:`DataTables` groupby: str,  same as init
    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    """

    def get_data(self) -> pd.DataFrame:
        """
        Method returns generated data
            :return: pd.DataFrame
        """
        return self._data

    def get_exp_or_imp_groupby(self) -> str:
        """
        If instance has attribute __exp_or_imp_groupby returns it, otherwise
        returns 'country_from'
        :return: str, __exp_or_imp_groupby or 'country_from' as default
        """
        try:
            return self.__exp_or_imp_groupby
        except:
            return 'country_from'

    def __init__(self, start_date: pd.Timestamp, end_date: pd.Timestamp,
                 measure: str, date_type: str, groupby: str, flow_type: str,
                 exporter_to_eu: str, exporter: str,
                 importer: str, selected_points: list):
        """
        Object initialization
            :param start_date: pd.Timestamp, period_from filter
            :param end_date:  pd.Timestamp, period_from filter
            :param measure: str, one of 'millions', 'billions'
            :param date_type: str, one of 'День', 'Неделя',
                'Месяц', 'Год'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
            :param groupby: str, one of 'default', 'country', 'sum'.
                Characteristics of grouping lines on the graph:
                'default' means group by certain point,
                'country' means group by country-exporter or country-importer,
                'sum' - summarize all supplies
            :param flow_type: str, one of 'default', 'net_flow', defines type
            of flows
            :param exporter_to_eu: str, shows chosen exporter_to_eu
            :param exporter: str, rus name of country-exporter
            :param importer: rus name of country-importer
            :param selected_points: list of str, english names of points
        """

        # Case when user choose 'ЕС' with groupby != sum graph will be very
        # bad and even broken, because there are a lot of lines on it
        # therefore be careful with that. I've added a logic to change
        # manually groupby in supply_time_val_page.py
        super().__init__(measure=measure, date_type=date_type,
                         groupby=groupby)
        self._set_start_date(start_date)
        self._set_end_date(end_date)
        self.__set_flow_type(flow_type)

        data = pd.DataFrame({})
        if exporter_to_eu is not None:
            if exporter_to_eu in CONST.GROUP_EXPORT:
                suitable_tables = CONST.GROUP_EXPORT[exporter_to_eu].copy()
            else:
                code = CONST.COUNTRY_CODE_DICT[exporter_to_eu]
                suitable_tables = self.__select_tables_exp_to_eu(code)
            if self.groupby == 'point':
                data = self.__gen_df_by_point(tables_list=suitable_tables,
                                              start_date=self._start_date,
                                              end_date=self._end_date,
                                              divider=self.divider,
                                              date_type=self.date_type)

            elif self.groupby == 'country':
                # Divide supplies with 1 exp and many importers by importer
                self.__exp_or_imp_groupby = 'country_to'
                data = self.__gen_df_by_country(tables_list=suitable_tables,
                                                start_date=self._start_date,
                                                end_date=self._end_date,
                                                divider=self.divider,
                                                date_type=self.date_type,
                                                exp_or_imp_groupby=self.__exp_or_imp_groupby
                                                )

            elif self.groupby == 'sum':
                data = self.__gen_df_by_sum(tables_list=suitable_tables,
                                            start_date=self._start_date,
                                            end_date=self._end_date,
                                            divider=self.divider,
                                            date_type=self.date_type)

        else:
            # Case gross flows, not net flows
            if flow_type == 'gross_flow':
                # Case when points are chosen
                if selected_points is not None:
                    suitable_tables = self.__select_tables_by_point(
                        point_names=selected_points)
                    if self.groupby == 'point':
                        data = self.__gen_df_by_point(
                            tables_list=suitable_tables,
                            start_date=self._start_date,
                            end_date=self._end_date, divider=self.divider,
                            date_type=self.date_type)
                        # Special point name for points
                        if not data.empty:
                            data['point'] = data['country_from'] + '\u279C' + \
                                data['country_to'] + ' (' + data[
                                'point'] + ')'
                    elif self.groupby == 'country':
                        self.__exp_or_imp_groupby = 'country_from'
                        data = self.__gen_df_by_country(tables_list=suitable_tables,
                                                        start_date=self._start_date,
                                                        end_date=self._end_date,
                                                        divider=self.divider,
                                                        date_type=self.date_type,
                                                        exp_or_imp_groupby=self.__exp_or_imp_groupby
                                                        )
                    elif self.groupby == 'sum':
                        data = self.__gen_df_by_sum(
                            tables_list=suitable_tables,
                            start_date=self._start_date,
                            end_date=self._end_date, divider=self.divider,
                            date_type=self.date_type)

                # Case when exporter/importer is chosen
                else:
                    suitable_tables = self.__select_tables_by_country(
                        exp_name=exporter,
                        imp_name=importer)
                    # exp_or_imp_groupby defines the way of aggregating data
                    if exporter is not None and importer is not None:
                        self.__exp_or_imp_groupby = 'country_from'
                    elif importer is not None:
                        self.__exp_or_imp_groupby = 'country_from'
                    else:
                        self.__exp_or_imp_groupby = 'country_to'
                    data = self.__gen_df(tables_list=suitable_tables,
                                         groupby=self.groupby,
                                         start_date=self._start_date,
                                         end_date=self._end_date,
                                         divider=self.divider,
                                         date_type=self.date_type,
                                         exp_or_imp_groupby=self.__exp_or_imp_groupby)

            # Case net flows
            elif flow_type == 'net_flow':
                # We can't use net flows with points, only with countries
                # importer is None always, so we have only exporter data here

                # Logic: get all export of country, then all import and then
                # subtract export from import
                self.__exp_or_imp_groupby = 'country_from'
                suitable_tables_exp = self.__select_tables_by_country(
                    exp_name=exporter, imp_name=None)
                export_data = self.__gen_df(tables_list=suitable_tables_exp,
                                            groupby=self.groupby,
                                            start_date=self._start_date,
                                            end_date=self._end_date,
                                            divider=self.divider,
                                            date_type=self.date_type,
                                            exp_or_imp_groupby=self.__exp_or_imp_groupby)
                self.__exp_or_imp_groupby = 'country_to'
                suitable_tables_imp = self.__select_tables_by_country(
                    exp_name=None, imp_name=exporter)
                import_data = self.__gen_df(tables_list=suitable_tables_imp,
                                            groupby=self.groupby,
                                            start_date=self._start_date,
                                            end_date=self._end_date,
                                            divider=self.divider,
                                            date_type=self.date_type,
                                            exp_or_imp_groupby=self.__exp_or_imp_groupby)

                data = \
                    self.__subtract_frames(df1=import_data, df2=export_data)

                # ! usually data is set in funcs like __gen_df_by_...,
                # but in case net_flows full dataframe is ready only here,
                # not in func, so need to set global_data here
                data['country'] = exporter
                data = data[['country'] + [item for item in data.columns if
                                           item != 'country']]
                self.__set_global_data(data)

        # suitable view of weeks on the graph including month and day,
        # available only for these tabs
        if date_type == 'Неделя':
            if not data.empty:
                data['period'] = data['period'] + ' (' + data[
                    'period_from'].str.slice(8, 10) + '/' + data[
                    'period_from'].str.slice(5, 7) + ')'

        self._data = data

    def __set_flow_type(self, flow_type: str) -> None:
        """
        Method sets _flow_type parameter to instance of the class
            :param flow_type: str, one of flow types: 'default', 'net_flow'
        """
        if flow_type is not None:
            self._flow_type = flow_type
        else:
            self._flow_type = 'gross_flow'

    @staticmethod
    def __set_global_data(dataframe: pd.DataFrame) -> None:
        """
        Sets current generated data to global variable (located in
        dash_app/global_vars.py)
            :param dataframe: current data
        """
        global_vars.CURRENT_GRAPH_DATA = dataframe.copy()

    @staticmethod
    def __select_tables_exp_to_eu(exporter_to_eu_code: str) -> list:
        """
        Chooses tables from CONST.FILES_NAME_LIST which have the same
        exporter code as exporter_to_eu_code
            :param exporter_to_eu_code: str, 2-sym code of country (e.g. 'RU')
            :return: list of names of tables
        """
        suitable_tables = []
        for item in CONST.FILES_NAME_LIST:
            type_point = item[6:11]
            exp_country_code = item[:2]
            imp_country_code = item[3:5]
            if imp_country_code != exp_country_code \
                    and imp_country_code in CONST.EU_CODES:
                if exp_country_code == exporter_to_eu_code and type_point in \
                        CONST.GAS_SUPPLY_POINTS:
                    suitable_tables.append(item)
        return suitable_tables

    @staticmethod
    def __select_tables_by_country(exp_name: str, imp_name: str) -> list:
        """
        Chooses tables from CONST.FILES_NAME_LIST which have the same
        exporter code as exp_name and imp name.
        Case when one of them is None (e.g. exp_name) func takes all tables
        with suitable another parameter (e.g. imp_name).
        If both parameters are equal to 'ЕС' returns empty list.
        If one of parameters is equal to 'ЕС' finds country codes from
        CONST.EU_CODES
            :param exp_name: str, rus name of country-exporter (e.g. 'Россия')
            :param imp_name: str, rus name of country-importer (e.g. 'Россия')
            :return: list of names of tables
        """
        suitable_tables = []

        if exp_name == imp_name == 'ЕС':
            pass

        elif exp_name == 'ЕС':
            if imp_name is not None:
                importer_code = CONST.COUNTRY_CODE_DICT[imp_name]
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    imp_country_code = item[3:5]
                    exp_country_code = item[:2]
                    if exp_country_code in CONST.EU_CODES \
                            and imp_country_code not in CONST.EU_CODES \
                            and type_point in CONST.GAS_SUPPLY_POINTS \
                            and imp_country_code == importer_code:
                        suitable_tables.append(item)
            else:
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    exp_country_code = item[:2]
                    if exp_country_code in CONST.EU_CODES and \
                            type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif imp_name == 'ЕС':
            if exp_name is not None:
                exporter_code = CONST.COUNTRY_CODE_DICT[exp_name]
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    imp_country_code = item[3:5]
                    exp_country_code = item[:2]
                    if imp_country_code in CONST.EU_CODES \
                            and exp_country_code not in CONST.EU_CODES \
                            and type_point in CONST.GAS_SUPPLY_POINTS \
                            and exp_country_code == exporter_code:
                        suitable_tables.append(item)
            else:
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    imp_country_code = item[3:5]
                    if imp_country_code in CONST.EU_CODES and \
                            type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif exp_name is not None and imp_name is not None:
            importer_code = CONST.COUNTRY_CODE_DICT[imp_name]
            exporter_code = CONST.COUNTRY_CODE_DICT[exp_name]
            for item in CONST.FILES_NAME_LIST:
                exp_country_code = item[:2]
                type_point = item[6:11]
                imp_country_code = item[3:5]
                if imp_country_code != exp_country_code:
                    if imp_country_code == importer_code and \
                            exp_country_code == exporter_code and \
                            type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif exp_name is not None:
            exporter_code = CONST.COUNTRY_CODE_DICT[exp_name]
            for item in CONST.FILES_NAME_LIST:
                exp_country_code = item[:2]
                type_point = item[6:11]
                imp_country_code = item[3:5]
                if imp_country_code != exp_country_code:
                    if exp_country_code == exporter_code and \
                            type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif imp_name is not None:
            importer_code = CONST.COUNTRY_CODE_DICT[imp_name]
            for item in CONST.FILES_NAME_LIST:
                type_point = item[6:11]
                exp_country_code = item[:2]
                imp_country_code = item[3:5]
                if imp_country_code != exp_country_code:
                    if imp_country_code == importer_code and \
                            type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        return suitable_tables

    @staticmethod
    def __select_tables_by_point(point_names: list) -> list:
        """
        Chooses tables from CONST.FILES_NAME_LIST which have the same
        point codes as codes of point names listed in point_names
            :param point_names: list of str, list of point names from
                entsog.eu (e.g. ['VTP - OTC - TGPS (PL)'])
            :return: list of names of tables
        """
        suitable_tables = []
        for item in point_names:
            cur_id = CONST.NAME_ID_DICT[item]
            for file_name in CONST.FILES_NAME_LIST:
                if cur_id in file_name:
                    cur_name = file_name
                    suitable_tables.append(cur_name)
        return suitable_tables

    @staticmethod
    def __subtract_frames(df1: pd.DataFrame, df2: pd.DataFrame) -> \
            pd.DataFrame:
        """
        function subtract df2['volume'] and df2['gas_KWh'] from
        df1['volume'] and df1['gas_KWh'] and returns frame with columns:
        period_from, period, volume, gcv_value, gas_KWh
            :param df1: pd.DataFrame
            :param df2: pd.DataFrame
            :return: pd.DataFrame
        """
        if df1.empty and df2.empty:
            return pd.DataFrame({})

        elif not df1.empty and df2.empty:
            pass
        elif df1.empty and not df2.empty:
            df2['volume'] = df2['volume'] * (-1)
            df2['gas_KWh'] = df2['gas_KWh'] * (-1)
            # return df2
        elif not df1.empty and not df2.empty:
            df2['volume'] = df2['volume'] * (-1)
            df2['gas_KWh'] = df2['gas_KWh'] * (-1)

        dataframe = pd.concat([df1, df2])
        dataframe = dataframe.groupby(['period_from', 'period'],
                                      as_index=False).agg(
            volume=('volume', np.sum),
            gas_KWh=('gas_KWh', np.sum),
        )
        dataframe = dataframe.sort_values(by=['period_from'])
        dataframe = dataframe.reset_index(drop=True)
        return dataframe

    @staticmethod
    def __gen_frame_from_sql(start_date: pd.Timestamp, end_date: pd.Timestamp,
                             table_name: str, divider=1,
                             date_type='День') -> pd.DataFrame:
        """
        Common method for all functions which generates frame. This method
        selects a sql request to database (there are no empty values in db's
        data, because it was fixed in to_agg func in
        download_data/download_no_vpn.py) and returns pd.DataFrame in certain
        format (name of columns)
        !!! when all volume values are equal to 0 then returns empty frame
            :param start_date: pd.Timestamp (period_from filter)
            :param end_date: pd.Timestamp (period_from filter)
            :param table_name: str, name of table (tables from
                agg_data_pages_csv, e.g. 'AT_HU_CTWIT_ex_21Z000000000003C')
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from

            :return: pd.DataFrame with data or empty frame when all volume
                values are equal to 0.
                Important:
                    column 'period_from' - pd.Timestamp.
                    column 'period' - str, special for date_type
                        (e.g. '22 января 2022)
        """
        country_from = CONST.CODE_COUNTRY_DICT[table_name[:2]]
        country_to = CONST.CODE_COUNTRY_DICT[table_name[3:5]]
        point_id = table_name[15:]
        point_name = CONST.ID_NAME_DICT[point_id]

        # Delete inappropriate symbols (if no - can be errors in sql database
        # request)
        point_name = point_name.replace('/', ' ')
        point_name = point_name.replace("'", '')
        point_type_short = table_name[6:11]
        point_type_name = CONST.SHORT_POINT_DICT[point_type_short]

        # Take one of many type names, divided by '/'
        if '/' in point_type_name:
            point_type_name = point_type_name.split('/')[0]

        start_prefix = f'''
            with main as (
                select 
                    coalesce(m.country_from, '{country_from}') as country_from,
                    coalesce(m.country_to, '{country_to}') as country_to,
                    '{point_name}' as point,
                    m.period_from, 
                    case when m.gcv_value is not Null and m.gcv_value > 0 
                        then round(m.gas_KWh/{divider}/m.gcv_value/1000000, 2)
                        else round(m.gas_KWh/{divider}/11.4/1000000, 2) end
                    as volume,
                    m.gcv_value,
                    m.gas_KWh, 
                    coalesce(m.point_type, '{point_type_name}') as point_type

                from table_{table_name.replace('-', '_')} as m
                where strftime('%Y-%m-%d', m.period_from) >= '{start_date.date()}'
                    and strftime('%Y-%m-%d', m.period_from) <= '{end_date.date()}'

                order by strftime('%Y-%m-%d', m.period_from)
            )
        '''

        if date_type == 'День':
            sql = start_prefix + f'''\n
                select 
                    t.country_from, t.country_to, t.point,
                    t.period_from,  
                    case 
                    when strftime('%m', t.period_from) = '01' then cast(strftime('%d', t.period_from) as integer)||' Января '|| strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '02' then cast(strftime('%d', t.period_from) as integer)||' Февраля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '03' then cast(strftime('%d', t.period_from) as integer)||' Марта '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '04' then cast(strftime('%d', t.period_from) as integer)||' Апреля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '05' then cast(strftime('%d', t.period_from) as integer)||' Мая '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '06' then cast(strftime('%d', t.period_from) as integer)||' Июня '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '07' then cast(strftime('%d', t.period_from) as integer)||' Июля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '08' then cast(strftime('%d', t.period_from) as integer)||' Августа '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '09' then cast(strftime('%d', t.period_from) as integer)||' Сентября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '10' then cast(strftime('%d', t.period_from) as integer)||' Октября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '11' then cast(strftime('%d', t.period_from) as integer)||' Ноября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '12' then cast(strftime('%d', t.period_from) as integer)||' Декабря '||strftime('%Y', t.period_from)
                    end as period,
                    t.volume,
                    t.gas_KWh

                from main as t
            '''

        # WARNING: Takes only full weeks!
        elif date_type == 'Неделя':
            # Having is used to throw out not full weeks
            # (ex. week starts at 29 of Dec)
            # case when is used to throw out cases
            # when some days are Null in one week
            sql = start_prefix + f'''\n 
                select 

                    t.country_from, t.country_to, t.point,
                    t.period_from,  
                    cast(strftime('%W', t.period_from) as integer)||strftime(' Неделя %Y', t.period_from) as period,

                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh

                from main as t
                group by strftime('%Y-%W', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = 7
                '''
        # WARNING: Takes only full months!
        elif date_type == 'Месяц':
            # Having is used to throw out not full months
            # case when is used to throw out cases when some
            # days are Null in one month
            sql = start_prefix + f'''\n 
                select 
                    t.country_from, t.country_to, t.point,
                    t.period_from,  
                    case when cast(strftime('%m', t.period_from) as integer) = 1 then 'Январь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 2 then 'Февраль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 3 then 'Март '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 4  then 'Апрель '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 5  then 'Май '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 6  then 'Июнь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 7  then 'Июль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 8  then 'Август '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 9  then 'Сентябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 10 then  'Октябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 11  then 'Ноябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 12  then 'Декабрь '||cast(strftime('%Y', t.period_from) as integer) end as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh

                from main as t
                group by strftime('%Y-%m', t.period_from)
                having count(coalesce(t.gas_KWh, 0)) = CAST(STRFTIME('%d', DATE(t.period_from,'start of month','+1 month','-1 day')) AS INTEGER)
            '''

        elif date_type == 'Год':
            sql = start_prefix + f'''\n 
                select 
                    t.country_from, t.country_to, t.point,
                    t.period_from,  
                    strftime('%Y', t.period_from) as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh

                from main as t
                group by strftime('%Y', t.period_from)
                order by t.period_from
            '''

        sql_frame = pd.read_sql(sql, con)

        # Check if frame is empty
        un_values = list(sql_frame['volume'].unique())
        nulls = [0, 0.0]
        if len(un_values) < 3:
            if un_values:
                if un_values[0] in nulls:
                    sql_frame = pd.DataFrame({})
            else:
                sql_frame = pd.DataFrame({})
        return sql_frame

    def __gen_df_by_point(self, tables_list: list, start_date: pd.Timestamp,
                          end_date: pd.Timestamp, divider=1,
                          date_type='День') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list and concatenates these frames into one
            :param tables_list: list of str, list of names of tables
                (e.g. ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """

        dataframe = pd.DataFrame({})
        if not tables_list:
            self.__set_global_data(dataframe)
        else:
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    start_date=start_date, end_date=end_date,
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            self.__set_global_data(dataframe)
        return dataframe

    def __gen_df_by_country(self, tables_list: list, start_date: pd.Timestamp,
                            end_date: pd.Timestamp, divider=1,
                            date_type='День',
                            exp_or_imp_groupby='country_from') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list, concatenates these frames into one and then
        group them by exp_or_imp_groupby, period_from and period columns
            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :param exp_or_imp_groupby: str, one of 'period_from', 'period_to'
            :return: pd.DataFrame
        """
        dataframe = pd.DataFrame({})
        if not tables_list:
            self.__set_global_data(dataframe)
        else:
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    start_date=start_date, end_date=end_date,
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            # dataframe can be empty even if table_list is not empty,
            # because when all volume values are equal to 0 gen_frame_from_sql
            # returns empty frame
            if not dataframe.empty:
                # sum data from different points with the same country_from
                # and country_to
                dataframe = dataframe.groupby([exp_or_imp_groupby,
                                               'period_from', 'period'],
                                              as_index=False).agg(
                    volume=('volume', np.sum),
                    gas_KWh=('gas_KWh', np.sum),
                )
                dataframe = dataframe.sort_values(by=['period_from',
                                                      exp_or_imp_groupby])
                dataframe = dataframe.reset_index(drop=True)
                # ! usually data is set in funcs like __gen_df_by_...,
                # but in case net_flows full dataframe is ready only
                # in __init__ not here, so need to set global_data in __init__
                self.__set_global_data(dataframe)

        return dataframe

    def __gen_df_by_sum(self, tables_list: list, start_date: pd.Timestamp,
                        end_date: pd.Timestamp, divider=1,
                        date_type='День') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list, concatenates these frames into one and then
        group them by period_from and period columns
            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter)
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """

        dataframe = pd.DataFrame({})
        if not tables_list:
            self.__set_global_data(dataframe)
        else:
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    start_date=start_date, end_date=end_date,
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            dataframe = dataframe.groupby(
                ['period_from', 'period'], as_index=False).agg(
                volume=('volume', np.sum),
                gas_KWh=('gas_KWh', np.sum),
            )
            dataframe = dataframe.sort_values(by=['period_from'])
            self.__set_global_data(dataframe)

        return dataframe

    def __gen_df(self, tables_list: list, groupby='point',
                 start_date=pd.Timestamp, end_date=pd.Timestamp, divider=1,
                 date_type='День', exp_or_imp_groupby='country_from') -> \
            pd.DataFrame:
        """
        Method generates pd.DataFrame depending on groupby. For particular
        groupby it calls certain method (gen_df_by_sum, ..by_country,
        ..by_point) and passes parameters to them.

            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param groupby: str, one of 'point', 'country', 'sum',
                characteristics of grouping lines on the graph:
                'point' means group by certain point,
                'country' means group by country-exporter or country-importer,
                'sum' - summarize all supplies
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :param exp_or_imp_groupby: str, one of 'period_from', 'period_to'
            :return: pd.DataFrame
        """
        data = pd.DataFrame({})
        if groupby == 'point':
            data = self.__gen_df_by_point(tables_list=tables_list,
                                          start_date=start_date,
                                          end_date=end_date,
                                          divider=divider,
                                          date_type=date_type)
        elif groupby == 'country':
            data = self.__gen_df_by_country(tables_list=tables_list,
                                            start_date=start_date,
                                            end_date=end_date,
                                            divider=divider,
                                            date_type=date_type,
                                            exp_or_imp_groupby=exp_or_imp_groupby)
        elif groupby == 'sum':
            data = self.__gen_df_by_sum(tables_list=tables_list,
                                        start_date=start_date,
                                        end_date=end_date,
                                        divider=divider,
                                        date_type=date_type)

        return data


class SupplyCompare(DataTables):
    """
    Class is associated with the 'Валовые поставки (сравнительные)'
    and 'Нетто поставки (сравнительные)' tabs
    and generates data which is using
    in creating 'Валовые поставки'
    and 'Нетто поставки' tabs graphs (SupplyTime)

    Attributes:

    - :class:`SupplyTime` _compare_years: list, same as in Init params
    - :class:`DataTables` date_type: str, same as init
    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    """

    def get_data(self) -> dict:
        """
        Method returns generated data in dict with pd.Dataframes
            :return: dict
        """
        return self._data

    def __init__(self, compare_years: str, measure: str,
                 date_type: str, flow_type: str, exporter_to_eu: str,
                 exporter: str, importer: str):
        """
        Object initialization
            :param compare_years: list of int, chosen compare years
            :param measure: str, one of 'millions', 'billions'
            :param date_type: str, one of 'День', 'Неделя',
                'Месяц'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
            :param flow_type: str, one of 'default', 'net_flow', defines type
                of flows
            :param exporter_to_eu: str, shows chosen exporter_to_eu
            :param exporter: str, rus name of country-exporter
            :param importer: rus name of country-importer
        """
        super().__init__(measure=measure,
                         date_type=date_type, groupby='')
        self.__set_flow_type(flow_type)
        self._set_compare_years(compare_years)
        if exporter_to_eu is not None:
            if exporter_to_eu in CONST.GROUP_EXPORT:
                suitable_tables = CONST.GROUP_EXPORT[exporter_to_eu].copy()
            else:
                code = CONST.COUNTRY_CODE_DICT[exporter_to_eu]
                suitable_tables = self.__select_tables_exp_to_eu(code)
            data = self.__gen_data_by_sum(tables_list=suitable_tables,
                                          compare_years=self._compare_years,
                                          divider=self.divider,
                                          date_type=self.date_type)
        else:
            if flow_type == 'gross_flow':
                # Case when exporter/importer is chosen
                suitable_tables = self.__select_tables_by_country(
                    exp_name=exporter,
                    imp_name=importer)
                data = self.__gen_data_by_sum(
                    tables_list=suitable_tables,
                    compare_years=self._compare_years,
                    divider=self.divider, date_type=self.date_type)

            elif flow_type == 'net_flow':
                # We can't use net flows with points, only with countries
                # importer is None always, so we have only exporter data here
                suitable_tables_exp = self.__select_tables_by_country(
                    exp_name=exporter,
                    imp_name=None)
                suitable_tables_imp = self.__select_tables_by_country(
                    exp_name=None,
                    imp_name=exporter)
                data = self.__gen_net_flow_data(suitable_tables_exp=suitable_tables_exp,
                                                suitable_tables_imp=suitable_tables_imp,
                                                divider=self.divider,
                                                date_type=self.date_type,
                                                compare_years=self._compare_years)

        self._data = data

    def __set_flow_type(self, flow_type: str) -> None:
        """
        Method sets _flow_type parameter to instance of the class
            :param flow_type: str, one of flow types: 'default', 'net_flow'
        """
        if flow_type is not None:
            self._flow_type = flow_type
        else:
            self._flow_type = 'gross_flow'

    @staticmethod
    def __set_global_data(dataframe: pd.DataFrame()) -> None:
        """
        Sets current generated data to global variable (located in
        dash_app/global_vars.py)
            :param dataframe: current data
        """
        global_vars.CURRENT_GRAPH_DATA = dataframe.copy()

    @staticmethod
    def __select_tables_exp_to_eu(exporter_to_eu_code: str) -> list:
        """
        Chooses tables from CONST.FILES_NAME_LIST which have the same
        exporter code as exporter_to_eu_code
            :param exporter_to_eu_code: str, 2-sym code of country (e.g. 'RU')
            :return: list of names of tables
        """
        suitable_tables = []
        for item in CONST.FILES_NAME_LIST:
            type_point = item[6:11]
            exp_country_code = item[:2]
            imp_country_code = item[3:5]
            if imp_country_code != exp_country_code \
                    and imp_country_code in CONST.EU_CODES:
                if exp_country_code == exporter_to_eu_code and \
                        type_point in CONST.GAS_SUPPLY_POINTS:
                    suitable_tables.append(item)
        return suitable_tables

    @staticmethod
    def __select_tables_by_country(exp_name: str, imp_name: str) -> list:
        """
        Chooses tables from CONST.FILES_NAME_LIST which have the same
        exporter code as exp_name and imp name.
        Case when one of them is None (e.g. exp_name) func takes all tables
        with suitable another parameter (e.g. imp_name).
        If both parameters are equal to 'ЕС' returns empty list.
        If one of parameters is equal to 'ЕС' finds country codes from
        CONST.EU_CODES
            :param exp_name: str, rus name of country-exporter (e.g. 'Россия')
            :param imp_name: str, rus name of country-importer (e.g. 'Россия')
            :return: list of names of tables
        """
        suitable_tables = []

        if exp_name == imp_name == 'ЕС':
            pass

        elif exp_name == 'ЕС':
            if imp_name is not None:
                importer_code = CONST.COUNTRY_CODE_DICT[imp_name]
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    imp_country_code = item[3:5]
                    exp_country_code = item[:2]
                    if exp_country_code in CONST.EU_CODES \
                            and imp_country_code not in CONST.EU_CODES \
                            and type_point in CONST.GAS_SUPPLY_POINTS \
                            and imp_country_code == importer_code:
                        suitable_tables.append(item)
            else:
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    exp_country_code = item[:2]
                    if exp_country_code in CONST.EU_CODES and type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif imp_name == 'ЕС':
            if exp_name is not None:
                exporter_code = CONST.COUNTRY_CODE_DICT[exp_name]
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    imp_country_code = item[3:5]
                    exp_country_code = item[:2]
                    if imp_country_code in CONST.EU_CODES \
                            and exp_country_code not in CONST.EU_CODES \
                            and type_point in CONST.GAS_SUPPLY_POINTS \
                            and exp_country_code == exporter_code:
                        suitable_tables.append(item)
            else:
                for item in CONST.FILES_NAME_LIST:
                    type_point = item[6:11]
                    imp_country_code = item[3:5]
                    if imp_country_code in CONST.EU_CODES and type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif exp_name is not None and imp_name is not None:
            importer_code = CONST.COUNTRY_CODE_DICT[imp_name]
            exporter_code = CONST.COUNTRY_CODE_DICT[exp_name]
            for item in CONST.FILES_NAME_LIST:
                exp_country_code = item[:2]
                type_point = item[6:11]
                imp_country_code = item[3:5]
                if imp_country_code != exp_country_code:
                    if imp_country_code == importer_code and exp_country_code == exporter_code and type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif exp_name is not None:
            exporter_code = CONST.COUNTRY_CODE_DICT[exp_name]
            for item in CONST.FILES_NAME_LIST:
                exp_country_code = item[:2]
                type_point = item[6:11]
                imp_country_code = item[3:5]
                if imp_country_code != exp_country_code:
                    if exp_country_code == exporter_code and type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        elif imp_name is not None:
            importer_code = CONST.COUNTRY_CODE_DICT[imp_name]
            for item in CONST.FILES_NAME_LIST:
                type_point = item[6:11]
                exp_country_code = item[:2]
                imp_country_code = item[3:5]
                if imp_country_code != exp_country_code:
                    if imp_country_code == importer_code and type_point in CONST.GAS_SUPPLY_POINTS:
                        suitable_tables.append(item)

        return suitable_tables

    @staticmethod
    def __subtract_frames(df1: pd.DataFrame, df2: pd.DataFrame,
                          date_type='День') -> pd.DataFrame:
        """
        function subtract df2['volume'] and df2['gas_KWh'] from
        df1['volume'] and df1['gas_KWh'] and returns frame with columns:
        period_from, period, volume, gcv_value, gas_KWh
            :param df1: pd.DataFrame
            :param df2: pd.DataFrame
            :param date_type: str, one of 'День', 'Неделя', 'Месяц'
            :return: pd.DataFrame
        """
        if df1.empty and df2.empty:
            return pd.DataFrame({})

        elif not df1.empty and df2.empty:
            pass
        elif df1.empty and not df2.empty:
            df2['volume'] = df2['volume'] * (-1)
            df2['gas_KWh'] = df2['gas_KWh'] * (-1)
            # return df2
        elif not df1.empty and not df2.empty:
            df2['volume'] = df2['volume'] * (-1)
            df2['gas_KWh'] = df2['gas_KWh'] * (-1)

        dataframe = pd.concat([df1, df2])
        dataframe = dataframe.groupby(['period_from', 'period'],
                                      as_index=False).agg(
            volume=('volume', np.sum),
            gas_KWh=('gas_KWh', np.sum),
        )
        dataframe = dataframe.sort_values(by=['period_from'])
        dataframe = dataframe.reset_index(drop=True)
        dataframe['period_from'] = pd.to_datetime(
            dataframe['period_from'], format='%Y-%m-%d')
        if date_type == 'День':
            dataframe['group_by'] = dataframe['period_from'].apply(
                lambda x: str(x)[5:])
        elif date_type == 'Неделя':
            # THAT WAY BECAUSE in years where the first week is not full sqlite3
            # counts it like a 0 week, but pandas like 1 week => diff in weeks
            dataframe['group_by'] = dataframe['period'].str[6:]
            dataframe['group_by'] = dataframe['group_by'].astype('int')
        elif date_type == 'Месяц':
            dataframe['group_by'] = dataframe['period_from'].dt.month

        dataframe['year'] = dataframe['period_from'].apply(
            lambda x: int(str(x)[:4]))
        global DATA
        DATA = dataframe.copy()
        dataframe = dataframe[
            ['year', 'group_by', 'period_from', 'period', 'volume', 'gas_KWh']]

        return dataframe

    @staticmethod
    def __gen_frame_from_sql(table_name: str, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Common method for all functions which generates frame. This method
        selects a sql request to database (there are no empty values in db's
        data, because it was fixed in to_agg func in
        download_data/download_no_vpn.py) and returns pd.DataFrame in certain
        format (name of columns)
        !!! when all volume values are equal to 0 then returns empty frame
            :param table_name: str, name of table (tables from
                agg_data_pages_csv, e.g. 'AT_HU_CTWIT_ex_21Z000000000003C')
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from

            :return: pd.DataFrame with data or empty frame when all volume
                values are equal to 0.
                Important:
                    column 'period_from' - pd.Timestamp.
                    column 'period' - str, special for date_type
                        (e.g. '22 января 2022)
        """
        start_prefix = f'''
            with main as (
                select 
                    m.period_from, 
                    case when m.gcv_value is not Null and m.gcv_value > 0 
                        then round(m.gas_KWh/{divider}/m.gcv_value/1000000, 2)
                        else round(m.gas_KWh/{divider}/11.4/1000000, 2) end
                    as volume,
                    m.gas_KWh 

                from table_{table_name.replace('-', '_')} as m
                where strftime('%Y-%m-%d', m.period_from) >= strftime('%Y-%m-%d', '2015-01-01')
                order by strftime('%Y-%m-%d', m.period_from)
            )
        '''

        if date_type == 'День':
            sql = start_prefix + f'''\n
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    strftime('%m-%d', t.period_from) as group_by,
                    t.period_from,  
                    case 
                    when strftime('%m', t.period_from) = '01' then cast(strftime('%d', t.period_from) as integer)||' Января'
                    when strftime('%m', t.period_from) = '02' then cast(strftime('%d', t.period_from) as integer)||' Февраля'
                    when strftime('%m', t.period_from) = '03' then cast(strftime('%d', t.period_from) as integer)||' Марта'
                    when strftime('%m', t.period_from) = '04' then cast(strftime('%d', t.period_from) as integer)||' Апреля'
                    when strftime('%m', t.period_from) = '05' then cast(strftime('%d', t.period_from) as integer)||' Мая'
                    when strftime('%m', t.period_from) = '06' then cast(strftime('%d', t.period_from) as integer)||' Июня'
                    when strftime('%m', t.period_from) = '07' then cast(strftime('%d', t.period_from) as integer)||' Июля'
                    when strftime('%m', t.period_from) = '08' then cast(strftime('%d', t.period_from) as integer)||' Августа'
                    when strftime('%m', t.period_from) = '09' then cast(strftime('%d', t.period_from) as integer)||' Сентября'
                    when strftime('%m', t.period_from) = '10' then cast(strftime('%d', t.period_from) as integer)||' Октября'
                    when strftime('%m', t.period_from) = '11' then cast(strftime('%d', t.period_from) as integer)||' Ноября'
                    when strftime('%m', t.period_from) = '12' then cast(strftime('%d', t.period_from) as integer)||' Декабря'
                    end as period,
                    t.volume,
                    t.gas_KWh

                from main as t
            '''
        # WARNING: Takes only full weeks!
        elif date_type == 'Неделя':
            # Having is used to throw out not full weeks (ex. week starts at 29 of Dec)
            # case when is used to throw out cases when some days are Null in one week
            sql = start_prefix + f'''\n 
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    cast(strftime('%W', t.period_from) as integer) as group_by,

                    t.period_from,  
                    'Неделя '||cast(strftime('%W', t.period_from) as integer) as period,

                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh

                from main as t
                group by strftime('%Y-%W', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = 7
                '''
        # WARNING: Takes only full months!
        elif date_type == 'Месяц':
            # Having is used to throw out not full months
            # case when is used to throw out cases when some days are Null in one month
            sql = start_prefix + f'''\n 
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    cast(strftime('%m', t.period_from) as integer) as group_by,
                    t.period_from,  
                    case when cast(strftime('%m', t.period_from) as integer) = 1 then 'Январь'
                    when cast(strftime('%m', t.period_from) as integer) = 2 then 'Февраль'
                    when cast(strftime('%m', t.period_from) as integer) = 3 then 'Март'
                    when cast(strftime('%m', t.period_from) as integer) = 4  then 'Апрель'
                    when cast(strftime('%m', t.period_from) as integer) = 5  then 'Май'
                    when cast(strftime('%m', t.period_from) as integer) = 6  then 'Июнь'
                    when cast(strftime('%m', t.period_from) as integer) = 7  then 'Июль'
                    when cast(strftime('%m', t.period_from) as integer) = 8  then 'Август'
                    when cast(strftime('%m', t.period_from) as integer) = 9  then 'Сентябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 10 then  'Октябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 11  then 'Ноябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 12  then 'Декабрь' end as period,

                    sum(coalesce(t.volume, 0)) as volume,


                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh


                from main as t
                group by strftime('%Y-%m', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = CAST(STRFTIME('%d', DATE(t.period_from,'start of month','+1 month','-1 day')) AS INTEGER)
            '''
        sql_frame = pd.read_sql(sql, con)

        # Check if frame hase only 0 in volumes (if it is so -> return
        # empty DataFrame).
        un_values = list(sql_frame['volume'].unique())
        nulls = [0, 0.0]
        if len(un_values) < 3:
            if un_values:
                if un_values[0] in nulls:
                    sql_frame = pd.DataFrame({})
            else:
                sql_frame = pd.DataFrame({})
        return sql_frame

    def __gen_data_by_sum(self, compare_years: list,
                          tables_list: list, divider=1,
                          date_type='День') -> \
            dict:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list and concatenates these frames into one
            :param compare_years: list of int, years chosen by user
            :param tables_list: list of str, list of names of tables
                (e.g. ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: dict, keys:
                'min': pd.DataFrame with min data for the 2015-2020 period
                'max': pd.DataFrame with max data for the 2015-2020 period
                 one of the year(int): pd.DataFrame with data for this chosen
                 year ...
                 other year:
                 other year...
        """
        dataframe = pd.DataFrame({})
        # min, max, years data
        data_by_years = {}

        if not tables_list:
            self.__set_global_data(dataframe)
        else:
            dataframe = pd.DataFrame({})
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            # sum data from different tables
            dataframe = dataframe.groupby(
                by=['period_from', 'period', 'group_by'], as_index=False).sum(
                ['volume', 'gas_KWh'])

            dataframe['year'] = pd.DatetimeIndex(dataframe['period_from']).year
            data = dataframe.copy()
            data['period_from'] = pd.to_datetime(
                data['period_from'], format='%Y-%m-%d')

            # Find max values from 2015 to 2020
            data_by_years['max'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).max(
                ['volume', 'gas_KWh'])
            data_by_years['max']['flag'] = 'max'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)

            # find min values from 2015 to 2020
            data_by_years['min'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).min(
                ['volume', 'gas_KWh'])
            data_by_years['min']['flag'] = 'min'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)

            for year in compare_years:
                data_by_years[year] = data[data['year'] == year]
                data_by_years[year].drop(columns=['period_from'], inplace=True)
                data_by_years[year]['flag'] = year
                data_by_years[year].sort_values(
                    by=['year', 'group_by'], ascending=True, inplace=True)

            # Global frame consist of all concatenated data from data_by_years
            self.__set_global_data(
                pd.concat([data_by_years[key] for key in data_by_years]))

        return data_by_years

    def __gen_net_flow_data(self, compare_years: list,
                            suitable_tables_exp: list,
                            suitable_tables_imp: list,
                            divider=1, date_type='День') -> dict:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in suitable_tables_exp and concatenates these frames into one,
        then the same procedure for suitable_tables_imp and then calculate
        difference: import minus export.
            :param compare_years: list of int, years chosen by user
            :param suitable_tables_exp: list of str, list of names of tables
                (e.g. ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param suitable_tables_imp: list of str, list of names of tables
                (e.g. ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: dict, keys:
                'min': pd.DataFrame with min data for the 2015-2020 period
                'max': pd.DataFrame with max data for the 2015-2020 period
                 one of the year(int): pd.DataFrame with data for this chosen
                 year ...
                 other year:
                 other year...
        """
        export_data = pd.DataFrame({})
        import_data = pd.DataFrame({})

        if suitable_tables_exp:
            for table in suitable_tables_exp:
                cur_frame = self.__gen_frame_from_sql(
                    table_name=table, divider=divider, date_type=date_type)
                export_data = pd.concat([export_data, cur_frame])

        if suitable_tables_imp:
            for table in suitable_tables_imp:
                cur_frame = self.__gen_frame_from_sql(
                    table_name=table, divider=divider, date_type=date_type)
                import_data = pd.concat([import_data, cur_frame])

        # WARNING: subract_frames also group it by period_from
        dataframe = self.__subtract_frames(import_data, export_data,
                                           date_type=date_type)

        # min, max, years data
        data_by_years = {}
        if dataframe.empty:
            self.__set_global_data(dataframe)
        else:
            data = dataframe.copy()
            data['period_from'] = pd.to_datetime(
                data['period_from'], format='%Y-%m-%d')

            # find max values from 2015 to 2020
            data_by_years['max'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).max(
                ['volume', 'gas_KWh'])
            data_by_years['max']['flag'] = 'max'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)

            # find min values from 2015 to 2020
            data_by_years['min'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).min(
                ['volume', 'gas_KWh'])
            data_by_years['min']['flag'] = 'min'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)

            for year in compare_years:
                data_by_years[year] = data[data['year'] == year]
                data_by_years[year].drop(columns=['period_from'], inplace=True)
                data_by_years[year]['flag'] = year
                data_by_years[year].sort_values(
                    by=['year', 'group_by'], ascending=True, inplace=True)

            # Global frame consist of all concatenated data from data_by_years
            self.__set_global_data(
                pd.concat([data_by_years[key] for key in data_by_years]))

        return data_by_years


class DemandTime(DataTables):
    """
    Class is associated with the 'Спрос', 'Спрос с прогнозом и температурой'
    tabs and generates data which is using
    in creating 'Спрос', 'Спрос с прогнозом и температурой' tabs
    graph (DemandTimeFig)

    Attributes:

    - :class:`SupplyTime` _start_date: pd.Timestamp, same as in Init params
    - :class:`SupplyTime` _end_date: pd.Timestamp, same as in Init params
    - :class:`DataTables` date_type: str, same as init
    - :class:`DataTables` groupby: str,  same as init
    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    """

    def get_data(self) -> dict:
        """
        Method returns generated data
            :return: dict
        """
        return self.__data

    def __init__(self, start_date: pd.Timestamp,
                 end_date: pd.Timestamp,
                 measure: str, date_type: str, groupby: str, countries: list,
                 show_temp: bool, show_forecast: bool):
        """
        Object initialization
            :param start_date: pd.Timestamp, period_from filter
            :param end_date:  pd.Timestamp, period_from filter
            :param measure: str, one of 'millions', 'billions'
            :param date_type: str, one of 'День', 'Неделя',
                'Месяц', 'Год'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
            :param groupby: str, one of 'country', 'sum'.
                Characteristics of grouping lines on the graph:
                'country' means group by country-exporter or country-importer,
                'sum' - summarize all supplies
            :param countries: list, names of demand countries
            :param show_temp: bool, if True: shows temperature graph
            :param show_forecast: bool, if True shows forecast graph
        """
        super().__init__(measure=measure, date_type=date_type, groupby=groupby)
        self._set_start_date(start_date)
        self._set_end_date(end_date)
        self.__data = {
            'gas_data': pd.DataFrame({}),
            'history_temp': pd.DataFrame({}),
            'forecast': pd.DataFrame({}),
        }
        countries = self._set_countries(countries)
        suitable_tables = self.__select_tables_by_countries(countries)

        if show_forecast:
            self.__data['forecast'] = self.__gen_forecast(countries,
                                                          self._start_date,
                                                          self._end_date)
        if show_temp:
            self.__data['history_temp'] = self.__gen_temp(countries,
                                                          self._start_date,
                                                          self._end_date)
        self.__data['gas_data'] = self.__gen_df(tables_list=suitable_tables,
                                                start_date=self._start_date,
                                                end_date=self._end_date,
                                                divider=self.divider,
                                                date_type=self.date_type)

        self.__set_global_data(datum=self.__data)

    @staticmethod
    def __set_global_data(datum: dict) -> None:
        """
        Sets current generated data to global variable
        CURRENT_GRAPH_DATA_DEMAND (located in dash_app/global_vars.py)
            :param datum: dict, current data, items:
                'gas_data': pd.DataFrame with demand data
                'history_temp': pd.DataFrame with history temperature data
                'forecast': pd.DataFrame with weather and demand forecast
        """

        # Case when there are gas_data and history temperature data
        if not datum['gas_data'].empty and not datum['history_temp'].empty:
            datum['gas_data']['period_from'] = pd.to_datetime(
                datum['gas_data']['period_from'])
            datum['history_temp']['time'] = pd.to_datetime(
                datum['history_temp']['time'])
            # Merging history temp and demand data
            data = pd.merge(left=datum['gas_data'],
                            right=datum['history_temp'],
                            left_on='period_from', right_on='time')
            data.drop(columns=['period_y', 'time'], inplace=True)
            data.rename(columns={'period_x': 'period'}, inplace=True)
            data['period_from'] = data['period_from'].dt.date
        # Case when there is only gas_data
        elif not datum['gas_data'].empty and datum['history_temp'].empty:
            data = datum['gas_data']
        # Case when there is only temperature data
        elif datum['gas_data'].empty and not datum['history_temp'].empty:
            data = datum['history_temp']
            data.rename({'time': 'period_from'})
        else:
            data = pd.DataFrame({})
        # If there is forecast
        if not datum['forecast'].empty:
            datum['forecast']['date'] = pd.to_datetime(
                datum['forecast']['date'])
            datum['forecast']['date'] = datum['forecast']['date'].dt.date

        global_vars.CURRENT_GRAPH_DATA_DEMAND['gas_data'] = data.copy()
        global_vars.CURRENT_GRAPH_DATA_DEMAND['forecast'] = datum['forecast']

    @staticmethod
    def __select_tables_by_countries(countries: list) -> list:
        """
        For demand exporter == importer, so
        Function chooses tables from CONST.FILES_NAME_LIST which have the same
        country_code (exporter == importer).
        If both parameters are equal to 'ЕС' returns empty list.
        If one of parameters is equal to 'ЕС' finds country codes from
        CONST.EU_CODES
            :param countries: list, rus name of countries with demand data
                (e.g. 'Россия')
            :return: list of names of tables
        """
        suitable_tables = []

        if 'ЕС' in countries:
            for item in CONST.FILES_NAME_LIST:
                type_point = item[6:11]
                exp_country_code = item[:2]
                imp_country_code = item[3:5]

                if exp_country_code == imp_country_code \
                        and imp_country_code in CONST.EU_CODES \
                        and type_point in CONST.GAS_CONSUMER_POINTS:
                    suitable_tables.append(item)
        else:
            if countries:
                for country in countries:
                    country_code = CONST.COUNTRY_CODE_DICT[country]
                    for item in CONST.FILES_NAME_LIST:
                        type_point = item[6:11]
                        exp_country_code = item[:2]
                        imp_country_code = item[3:5]
                        if exp_country_code == country_code and \
                                imp_country_code == country_code and \
                                type_point in CONST.GAS_CONSUMER_POINTS:
                            suitable_tables.append(item)
        return suitable_tables

    @staticmethod
    def __gen_frame_from_sql(start_date: pd.Timestamp, end_date: pd.Timestamp,
                             table_name: str, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Common method for all functions which generates frame. This method
        selects a sql request to database (there are no empty values in db's
        data, because it was fixed in to_agg func in
        download_data/download_no_vpn.py) and returns pd.DataFrame in certain
        format (name of columns)
        !!! when all volume values are equal to 0 then returns empty frame
            :param start_date: pd.Timestamp (period_from filter)
            :param end_date: pd.Timestamp (period_from filter)
            :param table_name: str, name of table (tables from
                agg_data_pages_csv, e.g. 'AT_HU_CTWIT_ex_21Z000000000003C')
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from

            :return: pd.DataFrame with data or empty frame when all volume
                values are equal to 0.
                Important:
                    column 'period_from' - pd.Timestamp.
                    column 'period' - str, special for date_type
                        (e.g. '22 января 2022)
        """
        # ex. of table_name: RS_BG_CTBEI_ex_58Z-000000007-KZ
        # in demand exporter == importer
        country = CONST.CODE_COUNTRY_DICT[table_name[:2]]
        point_id = table_name[15:]
        point_name = CONST.ID_NAME_DICT[point_id]

        # Delete inappropriate symbols (if no - can be errors in sql database
        # request)
        point_name = point_name.replace('/', ' ')
        point_name = point_name.replace("'", '')
        point_type_short = table_name[6:11]
        point_type_name = CONST.SHORT_POINT_DICT[point_type_short]

        # Take one of many type names, divided by '/'
        if '/' in point_type_name:
            point_type_name = point_type_name.split('/')[0]

        start_prefix = f'''
            with main as (
                select 
                    coalesce(m.country_from, '{country}') as country,
                    '{point_name}' as point,
                    m.period_from, 
                    case when m.gcv_value is not Null and m.gcv_value > 0 
                        then round(m.gas_KWh/{divider}/m.gcv_value/1000000, 2)
                        else round(m.gas_KWh/{divider}/11.4/1000000, 2) end
                    as volume,
                    m.gas_KWh, 
                    coalesce(m.point_type, '{point_type_name}') as point_type

                from table_{table_name.replace('-', '_')} as m
                where strftime('%Y-%m-%d', m.period_from) >= '{start_date.date()}'
                    and strftime('%Y-%m-%d', m.period_from) <= '{end_date.date()}'

                order by strftime('%Y-%m-%d', m.period_from)
            )
        '''

        if date_type == 'День':
            sql = start_prefix + f'''\n
                select 
                    t.country, t.point,
                    t.period_from,  
                    case 
                    when strftime('%m', t.period_from) = '01' then cast(strftime('%d', t.period_from) as integer)||' Января '|| strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '02' then cast(strftime('%d', t.period_from) as integer)||' Февраля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '03' then cast(strftime('%d', t.period_from) as integer)||' Марта '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '04' then cast(strftime('%d', t.period_from) as integer)||' Апреля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '05' then cast(strftime('%d', t.period_from) as integer)||' Мая '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '06' then cast(strftime('%d', t.period_from) as integer)||' Июня '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '07' then cast(strftime('%d', t.period_from) as integer)||' Июля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '08' then cast(strftime('%d', t.period_from) as integer)||' Августа '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '09' then cast(strftime('%d', t.period_from) as integer)||' Сентября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '10' then cast(strftime('%d', t.period_from) as integer)||' Октября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '11' then cast(strftime('%d', t.period_from) as integer)||' Ноября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '12' then cast(strftime('%d', t.period_from) as integer)||' Декабря '||strftime('%Y', t.period_from)
                    end as period,
                    t.volume,
                    t.gas_KWh,
                    t.point_type 

                from main as t
            '''
        # WARNING: Takes only full weeks!
        elif date_type == 'Неделя':
            # Having is used to throw out not full weeks (ex. week starts at 29 of Dec)
            # case when is used to throw out cases when some days are Null in one week
            sql = start_prefix + f'''\n 
                select 
                    t.country,
                    t.period_from,  
                    cast(strftime('%W', t.period_from) as integer)||strftime(' Неделя %Y', t.period_from) as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,
                    t.point_type

                from main as t
                group by strftime('%Y-%W', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = 7
                '''
        # WARNING: Takes only full months!
        elif date_type == 'Месяц':
            # Having is used to throw out not full months
            # case when is used to throw out cases when some days are Null in one month
            sql = start_prefix + f'''\n 
                select 
                    t.country, t.point,
                    t.period_from,  
                    case when cast(strftime('%m', t.period_from) as integer) = 1 then 'Январь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 2 then 'Февраль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 3 then 'Март '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 4  then 'Апрель '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 5  then 'Май '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 6  then 'Июнь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 7  then 'Июль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 8  then 'Август '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 9  then 'Сентябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 10 then  'Октябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 11  then 'Ноябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 12  then 'Декабрь '||cast(strftime('%Y', t.period_from) as integer) end as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,
                    t.point_type

                from main as t
                group by strftime('%Y-%m', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = CAST(STRFTIME('%d', DATE(t.period_from,'start of month','+1 month','-1 day')) AS INTEGER)
            '''

        elif date_type == 'Год':
            sql = start_prefix + f'''\n 
                select 
                    t.country, t.point,
                    t.period_from,  
                    strftime('%Y', t.period_from) as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,
                    t.point_type

                from main as t
                group by strftime('%Y', t.period_from)

                order by t.period_from
            '''

        sql_frame = pd.read_sql(sql, con)

        # Check if frame is empty
        un_values = list(sql_frame['volume'].unique())
        nulls = [0, 0.0]
        if len(un_values) < 3:
            if un_values:
                if un_values[0] in nulls:
                    sql_frame = pd.DataFrame({})
            else:
                sql_frame = pd.DataFrame({})
        return sql_frame

    def __gen_df_by_country(self, tables_list: list, start_date: pd.Timestamp,
                            end_date: pd.Timestamp, divider=1,
                            date_type='День') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list, concatenates these frames into one and then
        group them by country, period_from and period columns
            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """
        dataframe = pd.DataFrame({})
        if tables_list:
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    start_date=start_date, end_date=end_date,
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            if not dataframe.empty:
                dataframe = dataframe.groupby(['country', 'period_from',
                                               'period'], as_index=False).agg(
                    volume=('volume', np.sum),
                    gas_KWh=('gas_KWh', np.sum),
                )
                dataframe = dataframe.sort_values(by=['period_from'])
                dataframe = dataframe.reset_index(drop=True)

        return dataframe

    def __gen_df_by_sum(self, tables_list: list, start_date: pd.Timestamp,
                        end_date: pd.Timestamp, divider=1,
                        date_type='День') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list, concatenates these frames into one and then
        group them by period_from and period columns
            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter)
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """
        dataframe = pd.DataFrame({})
        if tables_list:
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    start_date=start_date, end_date=end_date,
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])
            if not dataframe.empty:
                dataframe = dataframe.groupby(
                    ['period_from', 'period'], as_index=False).agg(
                    volume=('volume', np.sum),
                    gas_KWh=('gas_KWh', np.sum),
                )
                dataframe = dataframe.sort_values(by=['period_from'])

        return dataframe

    def __gen_df(self, tables_list: list, start_date=pd.Timestamp,
                 end_date=pd.Timestamp, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Method generates pd.DataFrame depending on groupby. For particular
        groupby it calls certain method (gen_df_by_sum, ..by_country,
        ..by_point) and passes parameters to them.

            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """
        data = pd.DataFrame({})
        if self.groupby == 'country':
            data = self.__gen_df_by_country(tables_list=tables_list,
                                            start_date=start_date,
                                            end_date=end_date,
                                            divider=divider,
                                            date_type=date_type)
        elif self.groupby == 'sum':
            data = self.__gen_df_by_sum(tables_list=tables_list,
                                        start_date=start_date,
                                        end_date=end_date,
                                        divider=divider,
                                        date_type=date_type)

        return data

    @staticmethod
    def __gen_temp(countries, start_date: pd.Timestamp,
                   end_date: pd.Timestamp) -> pd.DataFrame:
        """
        Method generates pd.DataFrame with history temperature data. Data is
        given from 'weather_data/history_weather/'

            :param countries: list of str, list of rus names or countries
            (e.g. ['Россия'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :return: pd.DataFrame
        """
        if countries == 0:
            return pd.DataFrame({})
        else:
            temp_data = pd.DataFrame({})
            if 'ЕС' in countries:
                for code in CONST.EU_CODES:
                    file_exists = os.path.exists(
                        '../../weather_data/history_weather/' + code + '.csv')
                    if file_exists:
                        cur_temp_data = pd.read_csv(
                            '../../weather_data/history_weather/' + code +
                            '.csv')
                        cur_temp_data['time'] = pd.to_datetime(
                            cur_temp_data['time'], format='%Y-%m-%d')
                        cur_temp_data['time'] = cur_temp_data['time'].dt.date
                        cur_temp_data = cur_temp_data[
                            cur_temp_data['time'] >= start_date]
                        cur_temp_data = cur_temp_data[
                            cur_temp_data['time'] <= end_date]
                        temp_data = pd.concat([temp_data, cur_temp_data])
            else:
                for country in countries:
                    code = CONST.COUNTRY_CODE_DICT[country]
                    file_exists = os.path.exists(
                        '../../weather_data/history_weather/' + code + '.csv')
                    if file_exists:
                        cur_temp_data = pd.read_csv(
                            '../../weather_data/history_weather/' + code +
                            '.csv')
                        cur_temp_data['time'] = pd.to_datetime(
                            cur_temp_data['time'], format='%Y-%m-%d')
                        cur_temp_data['time'] = cur_temp_data['time'].dt.date
                        cur_temp_data = cur_temp_data[
                            cur_temp_data['time'] >= start_date]
                        cur_temp_data = cur_temp_data[
                            cur_temp_data['time'] <= end_date]
                        temp_data = pd.concat([temp_data, cur_temp_data])

            temp_data = temp_data.groupby(['time', 'period'],
                                          as_index=False).agg(
                tavg=('tavg', np.mean),
                tmin=('tmin', np.mean),
                tmax=('tmax', np.mean),
            )
            temp_data = temp_data.round({'tmin': 1,
                                         'tmax': 1,
                                         'tavg': 1})
            temp_data = temp_data.sort_values(by=['time'])
            temp_data = temp_data.reset_index(drop=True)
            return temp_data

    @staticmethod
    def __gen_forecast(countries: list, start_date: pd.Timestamp,
                       end_date: pd.Timestamp) -> pd.DataFrame:
        """
        Method generates pd.DataFrame with forecast temperature and
        gas demand data. Data is given from 'gas_data/demand_forecasts/'

            :param countries: list of str, list of rus names or countries
            (e.g. ['Россия'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :return: pd.DataFrame
        """
        if countries == 0:
            return pd.DataFrame({})
        else:
            forecast = pd.DataFrame({})

            if 'ЕС' in countries:
                for code in CONST.EU_CODES:
                    file_exists = os.path.exists(
                        '../../gas_data/demand_forecasts/' + code + '.csv')
                    if file_exists:
                        cur_forecast = pd.read_csv(
                            '../../gas_data/demand_forecasts/' + code + '.csv')
                        forecast = pd.concat([forecast, cur_forecast])
            else:
                for country in countries:
                    code = CONST.COUNTRY_CODE_DICT[country]
                    file_exists = os.path.exists(
                        '../../gas_data/demand_forecasts/' + code + '.csv')
                    if file_exists:
                        cur_forecast = pd.read_csv(
                            '../../gas_data/demand_forecasts/' + code + '.csv')
                        forecast = pd.concat([forecast, cur_forecast])

            if not forecast.empty:
                forecast['date'] = pd.to_datetime(forecast['date'],
                                                  format='%Y-%m-%d')
                forecast = forecast[
                    (forecast['date'] >= start_date) &
                    (forecast['date'] <= end_date)
                ]
                forecast = forecast.groupby(['date', 'period'],
                                            as_index=False).agg(
                    yhat_lower=('yhat_lower', np.sum),
                    yhat_upper=('yhat_upper', np.sum),
                    yhat=('yhat', np.sum),
                    tmin=('tmin', np.mean),
                    tmax=('tmax', np.mean),
                )
                forecast = forecast.round({'tmin': 1, 'tmax': 1})
                forecast = forecast.sort_values(by=['date'])
                forecast = forecast.reset_index(drop=True)

                # First row shows today therefore we don't need it
                # Last one shows data with the same tmax and tmin (bug of API)
                forecast = forecast.iloc[1:-1, :]
                return forecast


class DemandCompare(DataTables):
    """
    Class is associated with the 'Cпрос (сравнительный)' tab
    and generates data which is using
    in creating 'Спрос (сравнительный)' tab graph (DemandCompareFig)

    Attributes:

    - :class:`SupplyTime` _compare_years: list, same as in Init params
    - :class:`DataTables` date_type: str, same as init
    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    """

    def get_data(self) -> dict:
        """
        Method returns generated data
            :return: dict
        """
        return self.__data

    def __init__(self, compare_years: list,
                 measure: str, date_type: str, countries: list):
        """
        Object initialization
            :param compare_years: list of int, chosen compare years
            :param measure: str, one of 'millions', 'billions'
            :param date_type: str, one of 'День', 'Неделя',
                'Месяц'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
            :param countries: list, rus names of countries with demand
        """
        super().__init__(measure=measure, date_type=date_type, groupby='')
        self._set_compare_years(compare_years)
        countries = self._set_countries(countries)
        suitable_tables = self.__select_tables_by_countries(countries)

        self.__data = self.__gen_data_by_sum(tables_list=suitable_tables,
                                             compare_years=self._compare_years,
                                             divider=self.divider,
                                             date_type=self.date_type)

    @staticmethod
    def __set_global_data(dataframe: pd.DataFrame) -> None:
        """
        Sets current generated data to global variable (located in
        dash_app/global_vars.py)
            :param dataframe: current data
        """
        global_vars.CURRENT_GRAPH_DATA = dataframe.copy()

    @staticmethod
    def __select_tables_by_countries(countries: list) -> list:
        """
        For demand exporter == importer, so
        Function chooses tables from CONST.FILES_NAME_LIST which have the same
        country_code (exporter == importer).
        If both parameters are equal to 'ЕС' returns empty list.
        If one of parameters is equal to 'ЕС' finds country codes from
        CONST.EU_CODES
            :param countries: list, rus name of countries with demand data
                (e.g. 'Россия')
            :return: list of names of tables
        """
        suitable_tables = []

        if 'ЕС' in countries:
            for item in CONST.FILES_NAME_LIST:
                type_point = item[6:11]
                exp_country_code = item[:2]
                imp_country_code = item[3:5]

                if imp_country_code in CONST.EU_CODES and \
                        exp_country_code == imp_country_code and \
                        type_point in CONST.GAS_CONSUMER_POINTS:
                    suitable_tables.append(item)
        else:
            if countries:
                for country in countries:
                    country_code = CONST.COUNTRY_CODE_DICT[country]

                    for item in CONST.FILES_NAME_LIST:
                        type_point = item[6:11]
                        exp_country_code = item[:2]
                        imp_country_code = item[3:5]
                        if exp_country_code == country_code and \
                                imp_country_code == country_code and \
                                type_point in CONST.GAS_CONSUMER_POINTS:
                            suitable_tables.append(item)
        return suitable_tables

    @staticmethod
    def __gen_frame_from_sql(table_name, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Common method for all functions which generates frame. This method
        selects a sql request to database (there are no empty values in db's
        data, because it was fixed in to_agg func in
        download_data/download_no_vpn.py) and returns pd.DataFrame in certain
        format (name of columns)
        !!! when all volume values are equal to 0 then returns empty frame
            :param table_name: str, name of table (tables from
                agg_data_pages_csv, e.g. 'AT_HU_CTWIT_ex_21Z000000000003C')
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from

            :return: pd.DataFrame with data or empty frame when all volume
                values are equal to 0.
                Important:
                    column 'period_from' - pd.Timestamp.
                    column 'period' - str, special for date_type
                        (e.g. '22 января 2022)
        """
        # ex. of table_name: RS_BG_CTBEI_ex_58Z-000000007-KZ
        # in demand exporter == importer
        country = CONST.CODE_COUNTRY_DICT[table_name[:2]]
        point_id = table_name[15:]
        point_name = CONST.ID_NAME_DICT[point_id]

        # Delete inappropriate symbols (if no - can be errors in sql database
        # request)
        point_name = point_name.replace('/', ' ')
        point_name = point_name.replace("'", '')
        point_type_short = table_name[6:11]
        point_type_name = CONST.SHORT_POINT_DICT[point_type_short]

        # Take one of many type names, divided by '/'
        if '/' in point_type_name:
            point_type_name = point_type_name.split('/')[0]

        start_prefix = f'''
            with main as (
                select 
                    m.period_from, 
                    case when m.gcv_value is not Null and m.gcv_value > 0 
                        then round(m.gas_KWh/{divider}/m.gcv_value/1000000, 2)
                        else round(m.gas_KWh/{divider}/11.4/1000000, 2) end
                    as volume,
                    m.gas_KWh 

                from table_{table_name.replace('-', '_')} as m

                where strftime('%Y-%m-%d', m.period_from) >= strftime('%Y-%m-%d', '2015-01-01')

                order by strftime('%Y-%m-%d', m.period_from)
            )
        '''

        if date_type == 'День':
            sql = start_prefix + f'''\n
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    strftime('%m-%d', t.period_from) as group_by,
                    t.period_from,  
                    case 
                    when strftime('%m', t.period_from) = '01' then cast(strftime('%d', t.period_from) as integer)||' Января'
                    when strftime('%m', t.period_from) = '02' then cast(strftime('%d', t.period_from) as integer)||' Февраля'
                    when strftime('%m', t.period_from) = '03' then cast(strftime('%d', t.period_from) as integer)||' Марта'
                    when strftime('%m', t.period_from) = '04' then cast(strftime('%d', t.period_from) as integer)||' Апреля'
                    when strftime('%m', t.period_from) = '05' then cast(strftime('%d', t.period_from) as integer)||' Мая'
                    when strftime('%m', t.period_from) = '06' then cast(strftime('%d', t.period_from) as integer)||' Июня'
                    when strftime('%m', t.period_from) = '07' then cast(strftime('%d', t.period_from) as integer)||' Июля'
                    when strftime('%m', t.period_from) = '08' then cast(strftime('%d', t.period_from) as integer)||' Августа'
                    when strftime('%m', t.period_from) = '09' then cast(strftime('%d', t.period_from) as integer)||' Сентября'
                    when strftime('%m', t.period_from) = '10' then cast(strftime('%d', t.period_from) as integer)||' Октября'
                    when strftime('%m', t.period_from) = '11' then cast(strftime('%d', t.period_from) as integer)||' Ноября'
                    when strftime('%m', t.period_from) = '12' then cast(strftime('%d', t.period_from) as integer)||' Декабря'
                    end as period,
                    t.volume,
                    t.gas_KWh

                from main as t
            '''

        # WARNING: Takes only full weeks!
        elif date_type == 'Неделя':
            # Having is used to throw out not full weeks (ex. week starts at 29 of Dec)
            # case when is used to throw out cases when some days are Null in one week
            sql = start_prefix + f'''\n 
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    cast(strftime('%W', t.period_from) as integer) as group_by,

                    t.period_from,  
                    'Неделя '||cast(strftime('%W', t.period_from) as integer) as period,

                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh

                from main as t
                group by strftime('%Y-%W', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = 7
                '''
        # WARNING: Takes only full months!
        elif date_type == 'Месяц':
            # Having is used to throw out not full months
            # case when is used to throw out cases when some days are Null in one month
            sql = start_prefix + f'''\n 
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    cast(strftime('%m', t.period_from) as integer) as group_by,
                    t.period_from,  
                    case when cast(strftime('%m', t.period_from) as integer) = 1 then 'Январь'
                    when cast(strftime('%m', t.period_from) as integer) = 2 then 'Февраль'
                    when cast(strftime('%m', t.period_from) as integer) = 3 then 'Март'
                    when cast(strftime('%m', t.period_from) as integer) = 4  then 'Апрель'
                    when cast(strftime('%m', t.period_from) as integer) = 5  then 'Май'
                    when cast(strftime('%m', t.period_from) as integer) = 6  then 'Июнь'
                    when cast(strftime('%m', t.period_from) as integer) = 7  then 'Июль'
                    when cast(strftime('%m', t.period_from) as integer) = 8  then 'Август'
                    when cast(strftime('%m', t.period_from) as integer) = 9  then 'Сентябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 10 then  'Октябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 11  then 'Ноябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 12  then 'Декабрь' end as period,

                    sum(coalesce(t.volume, 0)) as volume,


                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh


                from main as t
                group by strftime('%Y-%m', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = CAST(STRFTIME('%d', DATE(t.period_from,'start of month','+1 month','-1 day')) AS INTEGER)
            '''

        sql_frame = pd.read_sql(sql, con)

        # Check if frame is empty
        un_values = list(sql_frame['volume'].unique())
        nulls = [0, 0.0]
        if len(un_values) < 3:
            if un_values:
                if un_values[0] in nulls:
                    sql_frame = pd.DataFrame({})
            else:
                sql_frame = pd.DataFrame({})
        return sql_frame

    def __gen_data_by_sum(self, tables_list: list, compare_years: list,
                          divider=1, date_type='День') -> dict:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list and concatenates these frames into one
            :param compare_years: list of int, years chosen by user
            :param tables_list: list of str, list of names of tables
                (e.g. ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: dict, keys:
                'min': pd.DataFrame with min data for the 2015-2020 period
                'max': pd.DataFrame with max data for the 2015-2020 period
                 one of the year(int): pd.DataFrame with data for this chosen
                 year ...
                 other year:
                 other year...
        """
        dataframe = pd.DataFrame({})
        # min, max, years data
        data_by_years = {}
        if not tables_list:
            self.__set_global_data(dataframe)
            data_by_years = {}
        else:
            dataframe = pd.DataFrame({})
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            # sum data from different tables
            dataframe = dataframe.groupby(
                by=['period_from', 'period', 'group_by'], as_index=False).agg(
                volume=('volume', np.sum),
                gas_KWh=('gas_KWh', np.sum),
            )

            dataframe['year'] = pd.DatetimeIndex(dataframe['period_from']).year
            data = dataframe.copy()
            data['period_from'] = pd.to_datetime(
                data['period_from'], format='%Y-%m-%d')
            data_by_years['max'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).max(
                ['volume', 'gas_KWh'])
            data_by_years['max']['flag'] = 'max'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)
            data_by_years['min'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).min(
                ['volume', 'gas_KWh'])
            data_by_years['min']['flag'] = 'min'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)

            for year in compare_years:
                data_by_years[year] = data[data['year'] == year]
                data_by_years[year].drop(columns=['period_from'], inplace=True)
                data_by_years[year]['flag'] = year
                data_by_years[year].sort_values(
                    by=['year', 'group_by'], ascending=True, inplace=True)

            # Global frame consist of all concatenated data from data_by_years
            self.__set_global_data(
                pd.concat([data_by_years[key] for key in data_by_years]))

        return data_by_years


class DemandTimeGroup(DataTables):
    """
    Class is associated with the 'Спрос по группам'
    tab and generates data which is using
    in creating 'Спрос по группам' tab graph (DemandTimeGroupFig)

    Attributes:

    - :class:`SupplyTime` _start_date: pd.Timestamp, same as in Init params
    - :class:`SupplyTime` _end_date: pd.Timestamp, same as in Init params
    - :class:`DataTables` date_type: str, same as init
    - :class:`DataTables` groupby: str,  same as init
    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    """

    def get_data(self) -> pd.DataFrame:
        """
        Method returns generated data
            :return: pd.DataFrame
        """
        return self.__data

    def __init__(self, start_date: pd.Timestamp,
                 end_date: pd.Timestamp,
                 measure: str, date_type: str,
                 countries: list, categories: list):
        """
        Object initialization
            :param start_date: pd.Timestamp, period_from filter
            :param end_date:  pd.Timestamp, period_from filter
            :param measure: str, one of 'millions', 'billions'
            :param date_type: str, one of 'День', 'Неделя',
                'Месяц', 'Год'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
            :param countries: list, rus names of countries with demand by group
            :param categories: list, categories of consumption for chosen
                country
        """
        super().__init__(measure=measure, date_type=date_type, groupby='')
        self._set_start_date(start_date)
        self._set_end_date(end_date)
        countries = self._set_countries(countries)
        categories = self._set_countries(categories)
        suitable_tables = self.__select_tables_by_countries(countries,
                                                            categories)
        self.__data = self.__gen_df_by_category(tables_dict=suitable_tables,
                                                start_date=self._start_date,
                                                end_date=self._end_date,
                                                divider=self.divider,
                                                date_type=self.date_type)

        self.__set_global_data(self.__data)

    @staticmethod
    def __set_global_data(dataframe: pd.DataFrame) -> None:
        """
        func sets current generated data to global variable (located in
        dash_app/global_vars.py)
            :param dataframe: current data
            :return: void
        """
        global_vars.CURRENT_GRAPH_DATA = dataframe.copy()

    @staticmethod
    def __select_tables_by_countries(countries: list, categories: list) -> \
            dict:
        """
        For demand exporter == importer, so
        Function chooses tables from CONST.UNIQUE_CONSUMERS_TYPES_RU
        which have the same country_code, then get list with tables names for
        every category and put it to suitable_tables[category] = list of tables
            :param countries: list, rus name of countries with demand data
                (e.g. 'Россия')
            :param categories: list, list of categories for chosen country
            :return: dict with categories as keys and lists of tables names as
                values
        """
        suitable_tables = {}
        if countries and categories:
            for country in countries:
                country_code = CONST.COUNTRY_CODE_DICT[country]
                for category in categories:
                    if category in CONST.UNIQUE_CONSUMERS_TYPES_RU[country]:
                        suitable_tables[category] = \
                            CONST.UNIQUE_CONSUMERS_TYPES_RU[country][category]
        return suitable_tables

    @staticmethod
    def __gen_frame_from_sql(start_date: pd.Timestamp, end_date: pd.Timestamp,
                             table_name: str, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Common method for all functions which generates frame. This method
        selects a sql request to database (there are no empty values in db's
        data, because it was fixed in to_agg func in
        download_data/download_no_vpn.py) and returns pd.DataFrame in certain
        format (name of columns)
        !!! when all volume values are equal to 0 then returns empty frame
            :param start_date: pd.Timestamp (period_from filter)
            :param end_date: pd.Timestamp (period_from filter)
            :param table_name: str, name of table (tables from
                agg_data_pages_csv, e.g. 'AT_HU_CTWIT_ex_21Z000000000003C')
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from

            :return: pd.DataFrame with data or empty frame when all volume
                values are equal to 0.
                Important:
                    column 'period_from' - pd.Timestamp.
                    column 'period' - str, special for date_type
                        (e.g. '22 января 2022)
        """
        # ex. of table_name: RS_BG_CTBEI_ex_58Z-000000007-KZ
        # in demand exporter == importer
        country = CONST.CODE_COUNTRY_DICT[table_name[:2]]
        point_id = table_name[15:]
        point_name = CONST.ID_NAME_DICT[point_id]

        # Delete inappropriate symbols (if no - can be errors in sql database
        # request)
        point_name = point_name.replace('/', ' ')
        point_name = point_name.replace("'", '')
        point_type_short = table_name[6:11]
        point_type_name = CONST.SHORT_POINT_DICT[point_type_short]

        # Take one of many type names, divided by '/'
        if '/' in point_type_name:
            point_type_name = point_type_name.split('/')[0]

        start_prefix = f'''
            with main as (
                select 
                    coalesce(m.country_from, '{country}') as country,
                    '{point_name}' as point,
                    m.period_from, 
                    case when m.gcv_value is not Null and m.gcv_value > 0 
                        then round(m.gas_KWh/{divider}/m.gcv_value/1000000, 2)
                        else round(m.gas_KWh/{divider}/11.4/1000000, 2) end
                    as volume,
                    m.gas_KWh, 
                    coalesce(m.point_type, '{point_type_name}') as point_type

                from table_{table_name.replace('-', '_')} as m
                where strftime('%Y-%m-%d', m.period_from) >= '{start_date.date()}'
                    and strftime('%Y-%m-%d', m.period_from) <= '{end_date.date()}'

                order by strftime('%Y-%m-%d', m.period_from)
            )
        '''

        if date_type == 'День':
            sql = start_prefix + f'''\n
                select 
                    t.country, t.point,
                    t.period_from,  
                    case 
                    when strftime('%m', t.period_from) = '01' then cast(strftime('%d', t.period_from) as integer)||' Января '|| strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '02' then cast(strftime('%d', t.period_from) as integer)||' Февраля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '03' then cast(strftime('%d', t.period_from) as integer)||' Марта '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '04' then cast(strftime('%d', t.period_from) as integer)||' Апреля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '05' then cast(strftime('%d', t.period_from) as integer)||' Мая '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '06' then cast(strftime('%d', t.period_from) as integer)||' Июня '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '07' then cast(strftime('%d', t.period_from) as integer)||' Июля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '08' then cast(strftime('%d', t.period_from) as integer)||' Августа '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '09' then cast(strftime('%d', t.period_from) as integer)||' Сентября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '10' then cast(strftime('%d', t.period_from) as integer)||' Октября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '11' then cast(strftime('%d', t.period_from) as integer)||' Ноября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '12' then cast(strftime('%d', t.period_from) as integer)||' Декабря '||strftime('%Y', t.period_from)
                    end as period,
                    t.volume,
                    t.gas_KWh,
                    t.point_type 

                from main as t
            '''

        # WARNING: Takes only full weeks!
        elif date_type == 'Неделя':
            # Having is used to throw out not full weeks (ex. week starts at 29 of Dec)
            # case when is used to throw out cases when some days are Null in one week
            sql = start_prefix + f'''\n 
                select 
                    t.country,
                    t.period_from,  
                    cast(strftime('%W', t.period_from) as integer)||strftime(' Неделя %Y', t.period_from) as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,
                    t.point_type

                from main as t
                group by strftime('%Y-%W', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = 7
                '''
        # WARNING: Takes only full months!
        elif date_type == 'Месяц':
            # Having is used to throw out not full months
            # case when is used to throw out cases when some days are Null in one month
            sql = start_prefix + f'''\n 
                select 
                    t.country, t.point,
                    t.period_from,  
                    case when cast(strftime('%m', t.period_from) as integer) = 1 then 'Январь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 2 then 'Февраль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 3 then 'Март '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 4  then 'Апрель '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 5  then 'Май '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 6  then 'Июнь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 7  then 'Июль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 8  then 'Август '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 9  then 'Сентябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 10 then  'Октябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 11  then 'Ноябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 12  then 'Декабрь '||cast(strftime('%Y', t.period_from) as integer) end as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,
                    t.point_type

                from main as t
                group by strftime('%Y-%m', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = CAST(STRFTIME('%d', DATE(t.period_from,'start of month','+1 month','-1 day')) AS INTEGER)
            '''

        elif date_type == 'Год':
            sql = start_prefix + f'''\n 
                select 
                    t.country, t.point,
                    t.period_from,  
                    strftime('%Y', t.period_from) as period,
                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,
                    t.point_type

                from main as t
                group by strftime('%Y', t.period_from)

                order by t.period_from
            '''

        sql_frame = pd.read_sql(sql, con)

        # Check if frame is empty. Frame has no NaN value in columns volume, gas_KWh
        un_values = list(sql_frame['volume'].unique())
        nulls = [0, 0.0]
        if len(un_values) < 3:
            if un_values:
                if un_values[0] in nulls:
                    sql_frame = pd.DataFrame({})
            else:
                sql_frame = pd.DataFrame({})
        return sql_frame

    def __gen_df_by_category(self, tables_dict: dict, start_date: pd.Timestamp,
                             end_date: pd.Timestamp, divider=1,
                             date_type='День') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        category and for every table in list (tables_dict[category])
        in tables_dict, concatenates these frames into one and then
        group them by consumer_type, period_from and period columns
            :param tables_dict: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """
        dataframe = pd.DataFrame({})
        if tables_dict:
            for consumer_type in tables_dict:
                for table in tables_dict[consumer_type]:
                    cur_frame = self.__gen_frame_from_sql(
                        start_date=start_date, end_date=end_date,
                        table_name=table, divider=divider, date_type=date_type)
                    cur_frame['consumer_type'] = consumer_type
                    dataframe = pd.concat([dataframe, cur_frame])

            if not dataframe.empty:
                dataframe = dataframe.groupby(['consumer_type', 'period_from',
                                               'period'], as_index=False).agg(
                    volume=('volume', np.sum),
                    gas_KWh=('gas_KWh', np.sum),
                )
                dataframe = dataframe.sort_values(by=['period_from',
                                                      'consumer_type'])
                dataframe = dataframe.reset_index(drop=True)

        return dataframe


class LngTime(DataTables):
    """
    Class is associated with the 'СПГ' tab and generates data which is using
    in creating 'СПГ' tab graph (LngTimeFig)

    Attributes:

    - :class:`SupplyTime` _start_date: pd.Timestamp, same as in Init params
    - :class:`SupplyTime` _end_date: pd.Timestamp, same as in Init params
    - :class:`DataTables` date_type: str, same as init
    - :class:`DataTables` groupby: str,  same as init
    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    """

    def get_data(self) -> pd.DataFrame:
        return self.__data

    def __init__(self, start_date: pd.Timestamp.date,
                 end_date: pd.Timestamp.date,
                 measure: str, date_type: str, groupby: str, countries: list):
        """
        Object initialization
            :param start_date: pd.Timestamp, period_from filter
            :param end_date:  pd.Timestamp, period_from filter
            :param measure: str, one of 'millions', 'billions'
            :param date_type: str, one of 'День', 'Неделя',
                'Месяц', 'Год'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
            :param groupby: str, one of 'country', 'sum'.
                Characteristics of grouping lines on the graph:
                'country' means group by country-exporter or country-importer,
                'sum' - summarize all supplies
            :param countries: list, names of demand countries
        """
        super().__init__(measure=measure, date_type=date_type, groupby=groupby)
        self._set_start_date(start_date)
        self._set_end_date(end_date)
        countries = self._set_countries(countries)
        suitable_tables = self.__select_tables_by_countries(countries)

        self.__data = self.__gen_df(tables_list=suitable_tables,
                                    start_date=self._start_date,
                                    end_date=self._end_date,
                                    divider=self.divider,
                                    date_type=self.date_type)

        self.__set_global_data(self.__data)

    @staticmethod
    def __set_global_data(dataframe: pd.DataFrame) -> None:
        """
        Sets current generated data to global variable (located in
        dash_app/global_vars.py)
            :param dataframe: current data
        """
        global_vars.CURRENT_GRAPH_DATA = dataframe.copy()

    @staticmethod
    def __select_tables_by_countries(countries: list) -> list:
        """
        For LNG exporter == importer, so
        Function chooses tables from CONST.FILES_NAME_LIST which have the same
        country_code (exporter == importer).
        If both parameters are equal to 'ЕС' returns empty list.
        If one of parameters is equal to 'ЕС' finds country codes from
        CONST.EU_CODES
            :param countries: list, rus name of countries with LNG data
                (e.g. 'Испания')
            :return: list of names of tables
        """
        suitable_tables = []

        if 'ЕС' in countries:
            for item in CONST.FILES_NAME_LIST:
                type_point = item[6:11]
                exp_country_code = item[:2]
                imp_country_code = item[3:5]

                if imp_country_code in CONST.EU_CODES and \
                        exp_country_code == imp_country_code and \
                        type_point in CONST.LNG_SUPPLY_POINTS:
                    suitable_tables.append(item)
        else:
            if countries:
                for country in countries:
                    country_code = CONST.COUNTRY_CODE_DICT[country]

                    for item in CONST.FILES_NAME_LIST:
                        type_point = item[6:11]
                        exp_country_code = item[:2]
                        imp_country_code = item[3:5]
                        if exp_country_code == country_code and \
                                imp_country_code == country_code and \
                                type_point in CONST.LNG_SUPPLY_POINTS:
                            suitable_tables.append(item)
        return suitable_tables

    @staticmethod
    def __gen_frame_from_sql(start_date: pd.Timestamp, end_date: pd.Timestamp,
                             table_name: str, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Common method for all functions which generates frame. This method
        selects a sql request to database (there are no empty values in db's
        data, because it was fixed in to_agg func in
        download_data/download_no_vpn.py) and returns pd.DataFrame in certain
        format (name of columns)
        !!! when all volume values are equal to 0 then returns empty frame
            :param start_date: pd.Timestamp (period_from filter)
            :param end_date: pd.Timestamp (period_from filter)
            :param table_name: str, name of table (tables from
                agg_data_pages_csv, e.g. 'AT_HU_CTWIT_ex_21Z000000000003C')
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from

            :return: pd.DataFrame with data or empty frame when all volume
                values are equal to 0.
                Important:
                    column 'period_from' - pd.Timestamp.
                    column 'period' - str, special for date_type
                        (e.g. '22 января 2022)
        """
        # ex. of table_name: RS_BG_CTBEI_ex_58Z-000000007-KZ
        # in demand exporter == importer
        country = CONST.CODE_COUNTRY_DICT[table_name[:2]]
        point_id = table_name[15:]
        point_name = CONST.ID_NAME_DICT[point_id]

        # Delete inappropriate symbols (if no - can be errors in sql database
        # request)
        point_name = point_name.replace('/', ' ')
        point_name = point_name.replace("'", '')
        point_type_short = table_name[6:11]
        point_type_name = CONST.SHORT_POINT_DICT[point_type_short]

        # Take one of many type names, divided by '/'
        if '/' in point_type_name:
            point_type_name = point_type_name.split('/')[0]

        start_prefix = f'''
            with main as (
                select 
                    coalesce(m.country_from, '{country}') as country,
                    '{point_name}' as point,

                    m.period_from, 
                    case when m.gcv_value is not Null and m.gcv_value > 0 
                        then round(m.gas_KWh/{divider}/m.gcv_value/1000000, 2)
                        else round(m.gas_KWh/{divider}/11.4/1000000, 2) end
                    as volume,
                    m.gcv_value,
                    m.gas_KWh, 

                    coalesce(m.point_type, '{point_type_name}') as point_type

                from table_{table_name.replace('-', '_')} as m

                where strftime('%Y-%m-%d', m.period_from) >= '{start_date.date()}'
                    and strftime('%Y-%m-%d', m.period_from) <= '{end_date.date()}'

                order by strftime('%Y-%m-%d', m.period_from)
            )
        '''

        if date_type == 'День':
            sql = start_prefix + f'''\n
                select 

                    t.country, t.point,
                    t.period_from,  
                    case 
                    when strftime('%m', t.period_from) = '01' then cast(strftime('%d', t.period_from) as integer)||' Января '|| strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '02' then cast(strftime('%d', t.period_from) as integer)||' Февраля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '03' then cast(strftime('%d', t.period_from) as integer)||' Марта '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '04' then cast(strftime('%d', t.period_from) as integer)||' Апреля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '05' then cast(strftime('%d', t.period_from) as integer)||' Мая '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '06' then cast(strftime('%d', t.period_from) as integer)||' Июня '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '07' then cast(strftime('%d', t.period_from) as integer)||' Июля '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '08' then cast(strftime('%d', t.period_from) as integer)||' Августа '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '09' then cast(strftime('%d', t.period_from) as integer)||' Сентября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '10' then cast(strftime('%d', t.period_from) as integer)||' Октября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '11' then cast(strftime('%d', t.period_from) as integer)||' Ноября '||strftime('%Y', t.period_from)
                    when strftime('%m', t.period_from) = '12' then cast(strftime('%d', t.period_from) as integer)||' Декабря '||strftime('%Y', t.period_from)
                    end as period,
                    t.volume,
                    t.gcv_value,
                    t.gas_KWh,
                    t.point_type 

                from main as t
            '''

        # WARNING: Takes only full weeks!
        elif date_type == 'Неделя':
            # Having is used to throw out not full weeks (ex. week starts at 29 of Dec)
            # case when is used to throw out cases when some days are Null in one week
            sql = start_prefix + f'''\n 
                select 

                    t.country,
                    t.period_from,  
                    cast(strftime('%W', t.period_from) as integer)||strftime(' Неделя %Y', t.period_from) as period,

                    sum(coalesce(t.volume, 0)) as volume,
                    coalesce(round(avg(t.gcv_value), 2), 11.4) as gcv_value,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,
                    t.point_type

                from main as t
                group by strftime('%Y-%W', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = 7
                '''
        # WARNING: Takes only full months!
        elif date_type == 'Месяц':
            # Having is used to throw out not full months
            # case when is used to throw out cases when some days are Null in one month
            sql = start_prefix + f'''\n 
                select 

                    t.country, t.point,
                    t.period_from,  
                    case when cast(strftime('%m', t.period_from) as integer) = 1 then 'Январь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 2 then 'Февраль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 3 then 'Март '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 4  then 'Апрель '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 5  then 'Май '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 6  then 'Июнь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 7  then 'Июль '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 8  then 'Август '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 9  then 'Сентябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 10 then  'Октябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 11  then 'Ноябрь '||cast(strftime('%Y', t.period_from) as integer)
                    when cast(strftime('%m', t.period_from) as integer) = 12  then 'Декабрь '||cast(strftime('%Y', t.period_from) as integer) end as period,

                    sum(coalesce(t.volume, 0)) as volume,

                    coalesce(round(avg(t.gcv_value), 2), 11.4) as gcv_value,

                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,

                    t.point_type

                from main as t
                group by strftime('%Y-%m', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = CAST(STRFTIME('%d', DATE(t.period_from,'start of month','+1 month','-1 day')) AS INTEGER)
            '''

        elif date_type == 'Год':
            sql = start_prefix + f'''\n 
                select 

                    t.country, t.point,
                    t.period_from,  
                    strftime('%Y', t.period_from) as period,

                    sum(coalesce(t.volume, 0)) as volume,

                    coalesce(round(avg(t.gcv_value), 2), 11.4) as gcv_value,

                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh,

                    t.point_type

                from main as t
                group by strftime('%Y', t.period_from)

                order by t.period_from
            '''

        sql_frame = pd.read_sql(sql, con)

        # Check if frame is empty.
        un_values = list(sql_frame['volume'].unique())
        nulls = [0, 0.0]
        if len(un_values) < 3:
            if un_values:
                if un_values[0] in nulls:
                    sql_frame = pd.DataFrame({})
            else:
                sql_frame = pd.DataFrame({})
        return sql_frame

    def __gen_df_by_country(self, tables_list: list, start_date: pd.Timestamp,
                            end_date: pd.Timestamp, divider=1,
                            date_type='День') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list, concatenates these frames into one and then
        group them by country, period_from and period columns
            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """
        dataframe = pd.DataFrame({})
        if tables_list:
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    start_date=start_date, end_date=end_date,
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            if not dataframe.empty:
                dataframe = dataframe.groupby(['country', 'period_from',
                                               'period'], as_index=False).agg(
                    volume=('volume', np.sum),
                    gas_KWh=('gas_KWh', np.sum),
                )
                dataframe = dataframe.sort_values(by=['period_from'])
                dataframe = dataframe.reset_index(drop=True)

        return dataframe

    def __gen_df_by_sum(self, tables_list: list, start_date: pd.Timestamp,
                        end_date: pd.Timestamp, divider=1,
                        date_type='День') -> pd.DataFrame:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list, concatenates these frames into one and then
        group them by period_from and period columns
            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter)
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """
        dataframe = pd.DataFrame({})
        if tables_list:
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    start_date=start_date, end_date=end_date,
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])
            if not dataframe.empty:
                dataframe = dataframe.groupby(
                    ['period_from', 'period'], as_index=False).sum('volume')
                dataframe = dataframe.sort_values(by=['period_from'])
            # ! Data without NaNs because in agg_data NaN values were changed to 0
            # dataframe['volume'].fillna(0, inplace=True)

        return dataframe

    def __gen_df(self, tables_list: list, start_date=pd.Timestamp,
                 end_date=pd.Timestamp, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Method generates pd.DataFrame depending on groupby. For particular
        groupby it calls certain method (gen_df_by_sum, ..by_country)
        and passes parameters to them.

            :param tables_list: list of str, list of names of tables (e.g.
                ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param start_date: pd.DataFrame (period_from filter)
            :param end_date: pd.DataFrame (period_from filter
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: pd.DataFrame
        """
        data = pd.DataFrame({})
        if self.groupby == 'country':
            data = self.__gen_df_by_country(tables_list=tables_list,
                                            start_date=start_date,
                                            end_date=end_date,
                                            divider=divider,
                                            date_type=date_type)

        elif self.groupby == 'sum':

            data = self.__gen_df_by_sum(tables_list=tables_list,
                                        start_date=start_date,
                                        end_date=end_date,
                                        divider=divider,
                                        date_type=date_type)
        return data


class LngCompare(DataTables):
    """
    Class is associated with the 'СПГ (сравнительный)' tab
    and generates data which is using
    in creating 'СПГ (сравнительный)' tab graph (LngCompareFig)

    Attributes:

    - :class:`SupplyTime` _compare_years: list, same as in Init params
    - :class:`DataTables` date_type: str, same as init
    - :class:`DataTables` divider: int, one of 1, 1000.
        If 1 -> millions of m3, if 1000 -> billions of m3
    """

    def get_data(self) -> dict:
        """
        Method returns generated data
            :return: dict
        """
        return self.__data

    def __init__(self, compare_years: list,
                 measure: str, date_type: str, countries: list):
        """
        Object initialization
            :param compare_years: list of int, chosen compare years
            :param measure: str, one of 'millions', 'billions'
            :param date_type: str, one of 'День', 'Неделя',
                'Месяц'. Defines dates format
                (e.g. show supplies at particular day (chosen 'День'),
                or show the sum of supplies for the week (chosen 'Неделя'))
            :param countries: list, rus names of countries with demand
        """
        super().__init__(measure=measure, date_type=date_type, groupby='')

        self._set_compare_years(compare_years)
        countries = self._set_countries(countries)
        suitable_tables = self.__select_tables_by_countries(countries)

        self.__data = self.__gen_data_by_sum(tables_list=suitable_tables,
                                             compare_years=self._compare_years,
                                             divider=self.divider,
                                             date_type=self.date_type)

    @staticmethod
    def __set_global_data(dataframe: pd.DataFrame) -> None:
        """
        Sets current generated data to global variable (located in
        dash_app/global_vars.py)
            :param dataframe: current data
        """
        global_vars.CURRENT_GRAPH_DATA = dataframe.copy()

    @staticmethod
    def __select_tables_by_countries(countries: list) -> list:
        """
        For LNG exporter == importer, so
        Function chooses tables from CONST.FILES_NAME_LIST which have the same
        country_code (exporter == importer).
        If both parameters are equal to 'ЕС' returns empty list.
        If one of parameters is equal to 'ЕС' finds country codes from
        CONST.EU_CODES
            :param countries: list, rus name of countries with LNG data
                (e.g. 'Испания')
            :return: list of names of tables
        """
        suitable_tables = []

        if 'ЕС' in countries:
            for item in CONST.FILES_NAME_LIST:
                type_point = item[6:11]
                exp_country_code = item[:2]
                imp_country_code = item[3:5]

                if imp_country_code in CONST.EU_CODES and \
                        exp_country_code == imp_country_code and \
                        type_point in CONST.LNG_SUPPLY_POINTS:
                    suitable_tables.append(item)
        else:
            if countries:
                for country in countries:
                    country_code = CONST.COUNTRY_CODE_DICT[country]

                    for item in CONST.FILES_NAME_LIST:
                        type_point = item[6:11]
                        exp_country_code = item[:2]
                        imp_country_code = item[3:5]
                        if exp_country_code == country_code and imp_country_code == country_code and type_point in CONST.LNG_SUPPLY_POINTS:
                            suitable_tables.append(item)
        return suitable_tables

    @staticmethod
    def __gen_frame_from_sql(table_name: str, divider=1, date_type='День') -> \
            pd.DataFrame:
        """
        Common method for all functions which generates frame. This method
        selects a sql request to database (there are no empty values in db's
        data, because it was fixed in to_agg func in
        download_data/download_no_vpn.py) and returns pd.DataFrame in certain
        format (name of columns)
        !!! when all volume values are equal to 0 then returns empty frame
            :param table_name: str, name of table (tables from
                agg_data_pages_csv, e.g. 'AT_HU_CTWIT_ex_21Z000000000003C')
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from

            :return: pd.DataFrame with data or empty frame when all volume
                values are equal to 0.
                Important:
                    column 'period_from' - pd.Timestamp.
                    column 'period' - str, special for date_type
                        (e.g. '22 января 2022)
        """
        # ex. of table_name: RS_BG_CTBEI_ex_58Z-000000007-KZ
        # in demand exporter == importer
        country = CONST.CODE_COUNTRY_DICT[table_name[:2]]
        point_id = table_name[15:]
        point_name = CONST.ID_NAME_DICT[point_id]

        # Delete inappropriate symbols (if no - can be errors in sql database
        # request)
        point_name = point_name.replace('/', ' ')
        point_name = point_name.replace("'", '')
        point_type_short = table_name[6:11]
        point_type_name = CONST.SHORT_POINT_DICT[point_type_short]

        # Take one of many type names, divided by '/'
        if '/' in point_type_name:
            point_type_name = point_type_name.split('/')[0]

        start_prefix = f'''
            with main as (
                select 

                    m.period_from, 
                    case when m.gcv_value is not Null and m.gcv_value > 0 
                        then round(m.gas_KWh/{divider}/m.gcv_value/1000000, 2)
                        else round(m.gas_KWh/{divider}/11.4/1000000, 2) end
                    as volume,
                    m.gas_KWh 

                from table_{table_name.replace('-', '_')} as m

                where strftime('%Y-%m-%d', m.period_from) >= strftime('%Y-%m-%d', '2015-01-01')

                order by strftime('%Y-%m-%d', m.period_from)
            )
        '''

        if date_type == 'День':
            sql = start_prefix + f'''\n
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    strftime('%m-%d', t.period_from) as group_by,
                    t.period_from,  
                    case 
                    when strftime('%m', t.period_from) = '01' then cast(strftime('%d', t.period_from) as integer)||' Января'
                    when strftime('%m', t.period_from) = '02' then cast(strftime('%d', t.period_from) as integer)||' Февраля'
                    when strftime('%m', t.period_from) = '03' then cast(strftime('%d', t.period_from) as integer)||' Марта'
                    when strftime('%m', t.period_from) = '04' then cast(strftime('%d', t.period_from) as integer)||' Апреля'
                    when strftime('%m', t.period_from) = '05' then cast(strftime('%d', t.period_from) as integer)||' Мая'
                    when strftime('%m', t.period_from) = '06' then cast(strftime('%d', t.period_from) as integer)||' Июня'
                    when strftime('%m', t.period_from) = '07' then cast(strftime('%d', t.period_from) as integer)||' Июля'
                    when strftime('%m', t.period_from) = '08' then cast(strftime('%d', t.period_from) as integer)||' Августа'
                    when strftime('%m', t.period_from) = '09' then cast(strftime('%d', t.period_from) as integer)||' Сентября'
                    when strftime('%m', t.period_from) = '10' then cast(strftime('%d', t.period_from) as integer)||' Октября'
                    when strftime('%m', t.period_from) = '11' then cast(strftime('%d', t.period_from) as integer)||' Ноября'
                    when strftime('%m', t.period_from) = '12' then cast(strftime('%d', t.period_from) as integer)||' Декабря'
                    end as period,
                    t.volume,
                    t.gas_KWh

                from main as t
            '''

        # WARNING: Takes only full weeks!
        elif date_type == 'Неделя':
            # Having is used to throw out not full weeks (ex. week starts at 29 of Dec)
            # case when is used to throw out cases when some days are Null in one week
            sql = start_prefix + f'''\n 
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    cast(strftime('%W', t.period_from) as integer) as group_by,

                    t.period_from,  
                    'Неделя '||cast(strftime('%W', t.period_from) as integer) as period,

                    sum(coalesce(t.volume, 0)) as volume,
                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh

                from main as t
                group by strftime('%Y-%W', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = 7
                '''
        # WARNING: Takes only full months!
        elif date_type == 'Месяц':
            # Having is used to throw out not full months
            # case when is used to throw out cases when some days are Null in one month
            sql = start_prefix + f'''\n 
                select 
                    cast(strftime('%Y', t.period_from) as integer) as year,
                    cast(strftime('%m', t.period_from) as integer) as group_by,
                    t.period_from,  
                    case when cast(strftime('%m', t.period_from) as integer) = 1 then 'Январь'
                    when cast(strftime('%m', t.period_from) as integer) = 2 then 'Февраль'
                    when cast(strftime('%m', t.period_from) as integer) = 3 then 'Март'
                    when cast(strftime('%m', t.period_from) as integer) = 4  then 'Апрель'
                    when cast(strftime('%m', t.period_from) as integer) = 5  then 'Май'
                    when cast(strftime('%m', t.period_from) as integer) = 6  then 'Июнь'
                    when cast(strftime('%m', t.period_from) as integer) = 7  then 'Июль'
                    when cast(strftime('%m', t.period_from) as integer) = 8  then 'Август'
                    when cast(strftime('%m', t.period_from) as integer) = 9  then 'Сентябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 10 then  'Октябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 11  then 'Ноябрь'
                    when cast(strftime('%m', t.period_from) as integer) = 12  then 'Декабрь' end as period,

                    sum(coalesce(t.volume, 0)) as volume,


                    sum(coalesce(t.gas_KWh, 0)) as gas_KWh


                from main as t
                group by strftime('%Y-%m', t.period_from)

                having count(coalesce(t.gas_KWh, 0)) = CAST(STRFTIME('%d', DATE(t.period_from,'start of month','+1 month','-1 day')) AS INTEGER)
            '''

        sql_frame = pd.read_sql(sql, con)

        # Check if frame is empty. Frame has no NaN value in columns volume, gas_KWh
        un_values = list(sql_frame['volume'].unique())
        nulls = [0, 0.0]
        if len(un_values) < 3:
            if un_values:
                if un_values[0] in nulls:
                    sql_frame = pd.DataFrame({})
            else:
                sql_frame = pd.DataFrame({})
        return sql_frame

    def __gen_data_by_sum(self, tables_list: list, compare_years: list,
                          divider=1, date_type='День', ) -> dict:
        """
        Method generates pd.DataFrame using __gen_frame_from_sql for every
        table in tables_list and concatenates these frames into one
            :param compare_years: list of int, years chosen by user
            :param tables_list: list of str, list of names of tables
                (e.g. ['AT_HU_CTWIT_ex_21Z000000000003C'])
            :param divider: int, 1 or 1000,characteristic of measurement:
                if 1 then in millions of m3, if 1000 then in billions of m3
            :param date_type: str, one of 'День', 'Неделя', 'Месяц', 'Год' -
                different types of grouping data by period_from
            :return: dict, keys:
                'min': pd.DataFrame with min data for the 2015-2020 period
                'max': pd.DataFrame with max data for the 2015-2020 period
                 one of the year(int): pd.DataFrame with data for this chosen
                 year ...
                 other year:
                 other year...
        """
        dataframe = pd.DataFrame({})
        # min, max, years data
        data_by_years = {}
        if not tables_list:
            self.__set_global_data(dataframe)
        else:
            dataframe = pd.DataFrame({})
            for table in tables_list:
                cur_frame = self.__gen_frame_from_sql(
                    table_name=table, divider=divider, date_type=date_type)
                dataframe = pd.concat([dataframe, cur_frame])

            # sum data from different tables
            dataframe = dataframe.groupby(
                by=['period_from', 'period', 'group_by'], as_index=False).agg(
                volume=('volume', np.sum),
                gas_KWh=('gas_KWh', np.sum),
            )

            dataframe['year'] = pd.DatetimeIndex(dataframe['period_from']).year
            data = dataframe.copy()
            data['period_from'] = pd.to_datetime(
                data['period_from'], format='%Y-%m-%d')
            data_by_years['max'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).max(
                ['volume', 'gas_KWh'])
            data_by_years['max']['flag'] = 'max'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)
            data_by_years['min'] = data[data['period_from'] < pd.Timestamp(
                '2020-01-01')].groupby(by=['group_by', 'period'],
                                       as_index=False).min(
                ['volume', 'gas_KWh'])
            data_by_years['min']['flag'] = 'min'
            data_by_years['max'].sort_values(
                by=['group_by'], ascending=True, inplace=True)

            for year in compare_years:
                data_by_years[year] = data[data['year'] == year]
                data_by_years[year].drop(columns=['period_from'], inplace=True)
                data_by_years[year]['flag'] = year
                data_by_years[year].sort_values(
                    by=['year', 'group_by'], ascending=True, inplace=True)

            # Global frame consist of all concatenated data from data_by_years
            self.__set_global_data(
                pd.concat([data_by_years[key] for key in data_by_years]))

        return data_by_years
