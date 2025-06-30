from .simd_r_drive_ws_client import (
    BaseDataStoreWsClient,
)

from typing import Union, Dict, Any, Optional, List


class DataStoreWsClient(BaseDataStoreWsClient):
    def batch_read_structured(
        self, data: Union[Dict[Any, bytes], List[Dict[Any, bytes]]]
    ) -> Union[Dict[Any, Optional[bytes]], List[Dict[Any, Optional[bytes]]]]:
        """
        Fetches values for a dict or list of dicts containing keys.

        This method accepts a dictionary or a list of dictionaries where values
        are keys in the datastore. It performs a high-performance batch read and
        returns a new object with the same structure, where each value is replaced
        with the corresponding fetched data.

        Args:
            data (Union[Dict[Any, bytes], List[Dict[Any, bytes]]]):
                A dictionary or list of dictionaries containing binary keys.

        Returns:
            Union[Dict[Any, Optional[bytes]], List[Dict[Any, Optional[bytes]]]]:
                A new object with the same shape as `data`, but with values replaced
                by the corresponding result (or None if the key was not found).
        """
        is_single_dict = isinstance(data, dict)
        dict_list = [data] if is_single_dict else data

        # Step 1: Decompile the structure to get a flat list of all datastore keys,
        # while remembering the original Python keys for later reconstruction.
        keys_to_fetch = []
        original_keys_map = []

        for d in dict_list:
            if not isinstance(d, dict):
                raise TypeError("All items in the list must be dictionaries.")

            original_keys = list(d.keys())
            original_keys_map.append(original_keys)
            keys_to_fetch.extend(d.values())

        # Step 2: Call the simple, fast Rust function with the flat list of keys.
        fetched_results = self.batch_read(keys_to_fetch)

        # Step 3: Rebuild the original structure with the new values.
        results_iterator = iter(fetched_results)
        new_dict_list = []

        for original_keys in original_keys_map:
            new_dict = {}
            for key in original_keys:
                new_dict[key] = next(results_iterator)
            new_dict_list.append(new_dict)

        # If the original input was a single dict, return a single dict.
        return new_dict_list[0] if is_single_dict else new_dict_list
