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
