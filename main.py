import modin.pandas as pd
# import pandas as pd
import pandas_market_calendars as mcal
import ray

ENTITIES = ['037833100', '594918104', '023135106', '30231G102', '478160104', '30303M102', '369604103', '00206R102',
            '46625H100', '742718109', '02079K305', '949746101', '931142103', '92343V104', '717081103', '084670702',
            '166764100', '191216100', '458140100', '58933Y105', '68389X105', '060505104', '20030N101', '437076102',
            '92826C839', '17275R102', '718172109', '713448108', '254687106', '459200101', '172967424', '91324P102',
            '031162100', '02209S103', '57636Q104', '88579Y101', '500754106', '806857108', '375558103', '00287Y109',
            '580135101', '126650100', '747525103', '110122108', '931427108', '532457108', '438516106', '913017109',
            '151020104', '097023105', '855244109', '907818108', '654106103', '911312106', '902973304', '741503403',
            '16119P108', '539830109', '882508104', '761713106', 'Y09827109', '38141G104', '609207105', '09062X103',
            '22160K105', '828806109', '194162103', '548661107', '026874784', '002824100', '617446448', '09247X101',
            '260543103', '025816109', '883556102', '887317303', '263534109', '65339F101', '293792107', '26441C204',
            '674599105', '235851102', '842587107', '00724F101', '26875P101', '37045V100', '79466L302', '872540109',
            '49456B101', '20825C104', '59156R108', '70450Y103', '149123101', '25746U109', '345370860', '03027X100',
            '369550108', '517834107', '61166W101', '693475105']


def main():
    nyse = mcal.get_calendar('NYSE')

    trade_date_range = nyse.valid_days(start_date='2016-12-20', end_date='2017-01-10')
    trade_date_range.name = "pricing_date"

    pricing_index = pd.MultiIndex.from_product([trade_date_range, ENTITIES], names=["pricing_date", "entity"])

    df = pd.DataFrame(index=pricing_index)

    # Following results in ValueError: Invalid integer data type 'f'.
    # filtered_df = df.loc['2017-01-09'].index.get_level_values("entity")
    filtered_df = df._default_to_pandas(lambda x: x.loc['2017-01-09']).index.get_level_values("entity")
    print(filtered_df)

    dates_df = pd.DataFrame(df.index.get_level_values("pricing_date")).drop_duplicates().sort_values(by="pricing_date")
    # Following results in TypeError: '>=' not supported between instances of 'str' and 'int'
    # print(dates_df.iloc[len(dates_df) // 2]["pricing_date"])
    pricing_date = dates_df.iloc[len(dates_df) // 2].squeeze()
    print(pricing_date)

    # As above, following results in ValueError: Invalid integer data type 'f'.
    # print(df.loc['2016-12-30'])
    print(df._default_to_pandas(lambda x: x.loc['2016-12-30']))


if __name__ == '__main__':
    ray.init()
    main()
