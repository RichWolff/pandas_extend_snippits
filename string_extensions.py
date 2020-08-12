@pd.api.extensions.register_dataframe_accessor("ext")
class SplitExplode:
    def __init__(self, series_object):
        self._obj = series_object

    def split_to_dataframe(
        self,
        column,
        splitstr,
        append_to_orig_df=False,
        remove_orig_column=False,
        trim_after_split=True,
        column_names=None,

    ):
        if not self._obj[column].dtype == np.dtype(np.object):
            raise AttributeError(f"Column: '{column}' is not a string column.") 

        # Get new df
        obj = self._obj.copy()
        s = obj[column].str.split(splitstr)

        if trim_after_split is True:
            s = s.apply(lambda x: [j.strip() for j in x])
        
        new_df = pd.DataFrame((zip_longest(*s.values))).T
        new_df.index = s.index
        new_df

        # Column Names
        if column_names is None:
            new_df.columns = [column + '_' + str(i) for i in new_df.columns]
        else:
            new_df.columns = column_names

        # Append to orig
        if append_to_orig_df is True:
            obj = pd.concat([obj, new_df], axis=1)
            if remove_orig_column is True:
                obj.drop(columns=column, inplace=True)
            return obj

        return new_df

    def split_explode(self, column, splitstr, trim_after_split=True):
        if not self._obj[column].dtype == np.dtype(np.object):
            raise AttributeError(f"Column: '{column}' is not a string column.")

        obj = self._obj.copy()
        obj[column] = obj[column].str.split(splitstr)
        obj = obj.explode(column)

        if trim_after_split is True:
            obj[column] = obj[column].str.strip()

        return obj
