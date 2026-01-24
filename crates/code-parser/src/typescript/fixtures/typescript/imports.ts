// eslint-disable-next-line @typescript-eslint/no-unused-vars
// @ts-nocheck
/* eslint-disable */

// =============================================================================
// COMPREHENSIVE TYPESCRIPT IMPORT TEST FILE
// =============================================================================
// This file contains examples of all import patterns to test the YAML rule

// -----------------------------------------------------------------------------
// 1. ALIAS IMPORTS - import { ORIGINAL as ALIAS } from 'SOURCE'
// -----------------------------------------------------------------------------
import { Component as ReactComponent } from 'react';
import { useState as useReactState, useEffect as useReactEffect } from 'react';
import { debounce as lodashDebounce, throttle as lodashThrottle } from 'lodash';
import { readFile as fsReadFile, writeFile as fsWriteFile } from 'fs/promises';

// -----------------------------------------------------------------------------
// 2. DEFAULT IMPORTS - import DEFAULT_NAME from 'SOURCE'
// -----------------------------------------------------------------------------
import React from 'react';
import axios from 'axios';
import express from 'express';
import lodash from 'lodash';
import moment from 'moment';
import chalk from 'chalk';

// -----------------------------------------------------------------------------
// 3. REGULAR NAMED IMPORTS - import { ORIGINAL } from 'SOURCE'
// -----------------------------------------------------------------------------
import { useState, useEffect, useCallback } from 'react';
import { map, filter, reduce } from 'lodash';
import { Router, Request, Response } from 'express';
import { readFileSync, writeFileSync } from 'fs';
import { join, resolve, dirname } from 'path';

// -----------------------------------------------------------------------------
// 4. MIXED IMPORTS (Default + Named)
// -----------------------------------------------------------------------------
import React, { useState } from 'react';
import React, { Component, useState, useEffect } from 'react';
import express, { Router, Request, Response, NextFunction } from 'express';
import lodash, { map, filter, debounce } from 'lodash';

// -----------------------------------------------------------------------------
// 5. MIXED IMPORTS (Default + Named + Alias)
// -----------------------------------------------------------------------------
import React, { Component as ReactComponent, useState as useReactState } from 'react';
import axios, { AxiosResponse as HttpResponse, AxiosError as HttpError } from 'axios';

// -----------------------------------------------------------------------------
// 6. NAMESPACE IMPORTS - import * as NAMESPACE_ALIAS from 'SOURCE'
// -----------------------------------------------------------------------------
import * as React from 'react';
import * as fs from 'fs';
import * as path from 'path';
import * as util from 'util';
import * as crypto from 'crypto';
import * as lodash from 'lodash';

// -----------------------------------------------------------------------------
// 7. SIDE EFFECT IMPORTS - import 'SOURCE'
// -----------------------------------------------------------------------------
import 'reflect-metadata';
import './styles.css';
import '../global.scss';
import 'dotenv/config';
import '@testing-library/jest-dom';
import 'zone.js/dist/zone';

// -----------------------------------------------------------------------------
// 8. DYNAMIC IMPORTS (Single Variable Assignment) - const VAR_NAME = require('SOURCE')
// -----------------------------------------------------------------------------
const fs = require('fs');
const path = require('path');
const express = require('express');
const lodash = require('lodash');
const moment = require('moment');

// With await
const asyncFs = await import('fs/promises');
const asyncPath = await import('path');

// -----------------------------------------------------------------------------
// 9. DYNAMIC IMPORTS (Destructured Shorthand) - const { ORIGINAL } = require('SOURCE')
// -----------------------------------------------------------------------------
const { readFile, writeFile } = require('fs/promises');
const { join, resolve } = require('path');
const { debounce, throttle } = require('lodash');
const { Router } = require('express');

// With await
const { useState, useEffect } = await import('react');
const { map, filter } = await import('lodash');

// -----------------------------------------------------------------------------
// 10. DYNAMIC IMPORTS (Destructured Alias) - const { ORIGINAL: ALIAS } = require('SOURCE')
// -----------------------------------------------------------------------------
const { readFile: fsRead, writeFile: fsWrite } = require('fs/promises');
const { join: pathJoin, resolve: pathResolve } = require('path');
const { debounce: lodashDebounce, throttle: lodashThrottle } = require('lodash');
const { Router: ExpressRouter, Request: ExpressRequest } = require('express');

// With await
const { useState: useReactState, useEffect: useReactEffect } = await import('react');
const { map: lodashMap, filter: lodashFilter } = await import('lodash');

// -----------------------------------------------------------------------------
// 11. MIXED DESTRUCTURING (Shorthand + Alias)
// -----------------------------------------------------------------------------
const { readFile, writeFile: fsWrite, createReadStream } = require('fs');
const { join, resolve: pathResolve, dirname } = require('path');

// With await
const { useState, useEffect: useReactEffect, useCallback } = await import('react');

// -----------------------------------------------------------------------------
// 12. DYNAMIC IMPORTS (Side Effect / Source Only) - require('SOURCE')
// -----------------------------------------------------------------------------
require('reflect-metadata');
require('dotenv/config');
require('./side-effects');
require('../polyfills');

// Not assigned to variable (should match SOURCE pattern)
import('zone.js/dist/zone');
import('./runtime-import');

// -----------------------------------------------------------------------------
// 13. COMPLEX NESTED PATTERNS
// -----------------------------------------------------------------------------

// Function scope imports
function setupApp() {
    const express = require('express');
    const { join } = require('path');
    return express();
}

// Conditional imports
if (process.env.NODE_ENV === 'development') {
    const devtools = require('redux-devtools');
    const { composeWithDevTools } = require('redux-devtools-extension');
}

// Block scope imports
{
    const localModule = require('./local-module');
    const { helper } = require('./helpers');
}

// Try-catch imports
try {
    const optionalDep = require('optional-dependency');
} catch (error) {
    console.log('Optional dependency not available');
}

// -----------------------------------------------------------------------------
// 14. IMPORT WITH UNUSUAL SOURCES
// -----------------------------------------------------------------------------
import { something } from '@scoped/package';
import utils from '@org/utils/deep/path';
import config from '../../config/app.config';
import styles from './Component.module.css';
import data from '../data.json';

// URL imports (modern)
import { fetch } from 'https://deno.land/std/http/mod.ts';

// -----------------------------------------------------------------------------
// 15. RE-EXPORTS (should also be detected)
// -----------------------------------------------------------------------------
export { Component as MyComponent } from 'react';
export { default as axios } from 'axios';
export * from 'lodash';
export * as utils from './utils';

// -----------------------------------------------------------------------------
// 16. IMPORT ASSERTIONS/ATTRIBUTES (TypeScript 4.5+)
// -----------------------------------------------------------------------------
import data from './data.json' assert { type: 'json' };
import styles from './styles.css' assert { type: 'css' };

// -----------------------------------------------------------------------------
// 17. TYPE-ONLY IMPORTS (TypeScript)
// -----------------------------------------------------------------------------
import type { FC, ReactNode } from 'react';
import type { Request, Response } from 'express';
import type { Config } from './types';

// Mixed type and value imports
import React, { type FC, useState } from 'react';
import axios, { type AxiosResponse } from 'axios';

// -----------------------------------------------------------------------------
// 18. DYNAMIC IMPORT WITH COMPLEX EXPRESSIONS
// -----------------------------------------------------------------------------
const moduleName = 'lodash';
const dynamicImport = await import(moduleName);
const conditionalImport = condition ? await import('module-a') : await import('module-b');

// -----------------------------------------------------------------------------
// 19. IMPORTS IN DIFFERENT CONTEXTS
// -----------------------------------------------------------------------------

// Class method
class MyClass {
    async loadModule() {
        const { helper } = await import('./helper');
        return helper;
    }
}

// Arrow function
const loadConfig = async () => {
    const config = await import('./config');
    return config.default;
};

// IIFE
(async () => {
    const { bootstrap } = await import('./bootstrap');
    bootstrap();
})();

// -----------------------------------------------------------------------------
// 20. EDGE CASES
// -----------------------------------------------------------------------------

// Multiple statements on one line (should each be detected)
const fs = require('fs'), path = require('path');

// Nested destructuring
const { promises: { readFile, writeFile } } = require('fs');
const { constants: { F_OK, R_OK } } = require('fs');

// With default values
const { port = 3000, host = 'localhost' } = require('./config');

// Rest patterns
const { first, ...rest } = require('./data');

// Array destructuring (less common but possible)
const [primary, secondary] = require('./colors');

// Comments and whitespace (should still work)
import { 
    // Core hooks
    useState, 
    useEffect,
    // Additional hooks
    useCallback 
} from 'react';

// Multi-line require
const express = require(
    'express'
);

export default {};
