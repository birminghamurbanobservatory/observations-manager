import {config} from '../config'; 
import Hashids from 'hashids/cjs';

const hasher = new Hashids(config.obs.salt);

export default hasher;